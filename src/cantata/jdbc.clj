(ns cantata.jdbc
  "Interaction with a JDBC data source

  See cantata.core for the main API entry point"
  (:require [cantata.data-source :as cds]
            [cantata.data-model :as cdm]
            [cantata.util :as cu :refer [throw-info]]
            [cantata.records :as r]
            [cantata.parse :as cpa]
            [cantata.query.util :as qu]
            [cantata.query.qualify :as qq]
            [clojure.java.jdbc :as jd]
            [honeysql.core :as hq]
            [clojure.string :as string]))

(set! *warn-on-reflection* true)

(defn dasherize [s]
  (string/replace s #"(?<!^|\.)_" "-"))

(defn undasherize [s]
  (string/replace s "-" "_"))

(defn ^:private get-row-fn [ds eq from-ent env]
  (let [ds-unmarshal (cds/get-row-unmarshaller ds)
        eselect (:select eq)]
    (if (and (seq eselect) (not (cdm/untyped? from-ent)))
      (let [eselect (into [] eselect)
            types (into [] (for [f eselect]
                             (-> f env :resolved :value :type)))
            joda-dates? (cds/get-option ds :joda-dates)]
        (comp (cpa/make-row-parser from-ent eselect types :joda-dates joda-dates?)
              ds-unmarshal))
      ds-unmarshal)))

(defn populate-sql-params [ds prepared-q params]
  (let [{:keys [param-values param-names]} prepared-q
        [sql & sql-params] (let [params (merge param-values params)]
                             (into [(:sql prepared-q)]
                                   (map #(get params %) param-names)))
        marshaller (cds/get-marshaller ds)]
    (into [sql] (map marshaller sql-params))))

(defn query
  "Implementation of cantata.core/query"
  [ds dm prepared-q callback & {:keys [params]}]
  (when-not (qu/prepared? prepared-q)
    (throw-info "Query must be prepared" {:q prepared-q}))
  (let [eq (:expanded-query prepared-q)
        {:keys [env added-paths]} prepared-q
        from-ent (cdm/entity dm (:from eq))
        [eq env added-paths] (cds/maybe-invoke-hook
                               [eq env added-paths]
                               ds from-ent :before-query eq env added-paths)
        jdbc-q (populate-sql-params ds prepared-q params)
        _ (when cu/*verbose* (prn jdbc-q))
        row-fn (get-row-fn ds eq from-ent env)
        [cols & rows] (jd/query ds jdbc-q
                                :identifiers dasherize
                                :row-fn row-fn
                                :as-arrays? true)
        qmeta {:cantata/query {:from from-ent
                               :env env
                               :expanded eq
                               :added-paths added-paths}}]
    (callback (with-meta cols qmeta)
              rows)))

(defn ^:private get-return-key [ent ret]
  (when ret
    (if-let [rk (:generated_key ret ((keyword "scope_identity()") ret))] ;H2 hack
      rk
      (let [pkfs (map #(cdm/field ent %) (cdm/normalize-pk (:pk ent)))
            ;; match clojure.java.jdbc identifier munging
            pkks (map #(-> % :db-name string/lower-case keyword) pkfs)
            pkvs (cdm/pk-val ret pkks)]
        (if (>= 1 (count pkvs))
          (first pkvs)
          pkvs)))))

(defn qualify-values-map [m quoting env marshaller]
  (into {} (for [[k v] m]
             [(hq/quote-identifier (get-in (env k) [:resolved :value :db-name] k)
                                   :style quoting)
              (if marshaller
                (marshaller v)
                v)])))

(defn insert!
  "Implementation of canata.core/insert!"
  [ds dm ename maps & {:as opts}]
  (let [ent (cdm/entity dm ename)
        fnames (cdm/field-names ent)
        no-fields? (empty? fnames)
        env (zipmap fnames (map #(cdm/resolve-path dm ent % :lax no-fields?)
                                fnames))
        quoting (cds/get-quoting ds)
        marshaller (cds/get-marshaller ds)
        maps* (map #(qualify-values-map % quoting env marshaller) maps)
        table (hq/quote-identifier (:db-name ent)
                                   :style quoting)
        cols (keys (first maps*))
        sql (str "INSERT INTO " table " (" (string/join ", " cols) ")"
                 " VALUES (" (string/join ", " (repeat (count cols) "?")) ")")
        param-groups (map vals maps*)
        ret-keys? (not (false? (:return-keys opts)))
        ret (if ret-keys?
              (doall (for [param-group (map vals maps*)]
                       (jd/db-do-prepared-return-keys
                         ds true sql param-group)))
              (apply jd/db-do-prepared ds true sql param-groups))]
    (when ret-keys?
      (map #(get-return-key ent %) ret))))

(defn update!
  "Implementation of canata.core/update!"
  [ds dm ename values pred & {:as opts}]
  (let [ent (cdm/entity dm ename)
        fnames (cdm/field-names ent)
        no-fields? (empty? fnames)
        env (zipmap fnames (map #(cdm/resolve-path dm ent % :lax no-fields?)
                                fnames))
        quoting (cds/get-quoting ds)
        marshaller (cds/get-marshaller ds)
        values* (qualify-values-map values quoting env marshaller)
        table {(hq/quote-identifier (:db-name ent)
                                    :style quoting)
               (hq/quote-identifier ename :style quoting)}
        ;; TODO: joins
        pred* (-> pred
                (qq/qualify-pred-fields quoting env)
                (hq/format-predicate :quoting quoting))]
    (first
      (jd/update! ds table values* pred*))))

(defn delete!
  "Implementation of canata.core/delete!"
  ([ds dm ename]
    (delete! ds dm ename nil))
  ([ds dm ename pred & {:as opts}]
    (let [ent (cdm/entity dm ename)
          fnames (cdm/field-names ent)
          no-fields? (empty? fnames)
          env (zipmap fnames (map #(cdm/resolve-path dm ent % :lax no-fields?)
                                  fnames))
          quoting (cds/get-quoting ds)
          table (hq/quote-identifier (:db-name ent) :style quoting)
          alias (hq/quote-identifier ename :style quoting)
          ;; TODO: joins
          [pred* & params] (when pred
                             (-> pred
                               (qq/qualify-pred-fields quoting env)
                               (hq/format-predicate :quoting quoting)))
          ;; HACK
          mysql? (= "mysql" (cds/get-subprotocol ds))
          sql (str "DELETE " (when mysql? alias) " FROM " table " AS " alias
                   (when pred* (str " WHERE " pred*)))
          sql-params (into [sql] params)]
      (first
        (jd/execute! ds sql-params)))))

;;;;

;; clojure.java.jdbc hack to allow query logging

(def ^:private db-do-prepared jd/db-do-prepared)

(defn db-do-prepared-hook [db transaction? & [sql & param-groups :as opts]]
  (if (string? transaction?)
    (prn [transaction?])
    (prn (into [sql] param-groups)))
  (apply db-do-prepared db transaction? opts))

(def ^:private db-do-prepared-return-keys jd/db-do-prepared-return-keys)

(defn db-do-prepared-return-keys-hook
  ([db sql param-group]
    (db-do-prepared-return-keys db sql param-group))
  ([db transaction? sql param-group]
    (when (jd/db-find-connection db)
      (prn (into [sql] param-group)))
    (db-do-prepared-return-keys db transaction? sql param-group)))