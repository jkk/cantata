(ns cantata.sql
  "Functions for interacting with a JDBC/SQL data source and readying queries
  for execution

  See cantata.core for the main API entry point"
  (:require [cantata.data-source :as cds]
            [cantata.data-model :as cdm]
            [cantata.query :as cq]
            [cantata.util :as cu :refer [throw-info]]
            [cantata.records :as r]
            [cantata.parse :as cpa]
            [clojure.java.jdbc :as jd]
            [honeysql.core :as hq]
            [clojure.string :as string])
  (:import cantata.records.PreparedQuery
           [honeysql.types SqlCall SqlRaw]))

(set! *warn-on-reflection* true)

(defn identifier
  "Turns an optionally-qualified string or keyword into a quoted identifier"
  ([x quoting]
    (hq/raw (hq/quote-identifier x :style quoting :split false)))
  ([x y quoting]
    (if-not x
      (identifier y quoting)
      (hq/raw (str (hq/quote-identifier x :style quoting :split false)
                   "."
                   (hq/quote-identifier y :style quoting :split false))))))

(defmulti qualify
  "Transforms a ResolvedPath or other object into a fully-qualified, quoted
  identifier or other SQL value, taking into account entity and field name
  mappings"
  (fn [x quoting]
    (-> x :resolved :type)))

(defmethod qualify :default [x quoting]
  x)

(defmethod qualify :joined-entity [x quoting]
  (let [ent (-> x :resolved :value)]
    (identifier (:db-schema ent) (:db-name ent) quoting)))

(defmethod qualify :entity [x quoting]
  (let [ent (-> x :resolved :value)]
    (identifier (:db-schema ent) (:db-name ent) quoting)))

(defmethod qualify :field [x quoting]
  (let [fname (-> x :resolved :value :db-name)
        chain (:chain x)]
    (if (seq chain)
      (identifier (-> chain peek :to-path) fname quoting)
      (identifier (-> x :root :name) fname quoting))))

(defmethod qualify :joined-field [x quoting]
  (if-let [fname (-> x :resolved :value :db-name)]
    (identifier (-> x :final-path cu/unqualify first) fname quoting)
    (let [[qual basename] (cu/unqualify (:final-path x))]
      (identifier qual basename quoting))))

(defmethod qualify :agg-op [x quoting]
  (let [v (-> x :resolved :value)
        fname (-> v :resolved-path :value :db-name)
        chain (:chain x)]
    (hq/call (:op v)
             (if (= :* (:path v))
               (hq/raw "*")
               (if (seq chain)
                 (if (cq/wildcard? (:path v))
                   (hq/raw (str (hq/quote-identifier (-> chain peek :to-path)
                                                     :style quoting :split false)
                                ".*"))
                   (identifier (-> chain peek :to-path) fname quoting))
                 (identifier (-> x :root :name) fname quoting))))))

(defmethod qualify :param [x quoting]
  (-> x :resolved :value))

(defmulti qualify-clause
  "Qualifies and quotes all identifiers in a query clause, taking into account
  entity and field name mappings"
  (fn [clause clause-val quoting env]
    clause))

(defmethod qualify-clause :default [_ cval _ _]
  cval)

(defmethod qualify-clause :from [_ from quoting env]
  (if-let [ent (-> (env from) :resolved :value)]
    [[(identifier (:db-schema ent) (:db-name ent) quoting) from]]
    [from]))

(declare qualify-query)

(defn ^:private get-subquery-env [subq]
  (:cantata.query/env (meta subq)))

(defmethod qualify-clause :select [_ select quoting env]
  (for [path select]
    (let [[field alias] (if (vector? path)
                          path
                          [path])
          qfield (if (map? field)
                   (qualify-query field quoting (or (get-subquery-env field) env))
                   (qualify (env field field) quoting))]
      (if (or (= :* field) (cq/wildcard? field))
        qfield
        (if (or (instance? SqlRaw field) (instance? SqlCall field)
                (number? field) (string? field))
          path
          [qfield (or alias (identifier (or (:final-path (env field)) field)
                                        quoting))])))))

(defn qualify-pred-fields [pred quoting env]
  (when pred
    (let [fields (cq/get-predicate-fields pred)
          smap (into {} (for [f fields]
                          [f (if (map? f)
                               (qualify-query f quoting (or (get-subquery-env f) env))
                               (qualify (env f f) quoting))]) )]
      (cq/replace-predicate-fields pred smap))))

(defmethod qualify-clause :where [_ where quoting env]
 (qualify-pred-fields where quoting env))

(defmethod qualify-clause :having [_ where quoting env]
 (qualify-pred-fields where quoting env))

(defmethod qualify-clause :order-by [_ order-by quoting env]
 (when order-by
   (for [f order-by]
     (let [fname (if (coll? f) (first f) f)
           dir (when (coll? f) (second f))
           qfield (qualify (env fname fname) quoting)]
       (if dir
         [qfield dir]
         qfield)))))

(defmethod qualify-clause :group-by [_ group-by quoting env]
 (when group-by
   (for [fname group-by]
     (qualify (env fname fname) quoting))))

;; TODO: subqueries
(defn ^:private qualify-join [joins quoting env]
 (mapcat (fn [[to on]]
           (let [ename (if (vector? to) (first to) to)
                 path (if (vector? to)
                        (second to)
                        ename)
                 qpath (identifier path quoting)
                 qename (if (map? ename)
                          (qualify-query ename quoting (or (get-subquery-env ename) env))
                          (qualify (env path path) quoting))
                 on* (qualify-pred-fields on quoting env)]
             [[qename qpath] on*]))
         (partition 2 joins)))

(defmethod qualify-clause :join [_ joins quoting env]
 (qualify-join joins quoting env))

(defmethod qualify-clause :left-join [_ joins quoting env]
 (qualify-join joins quoting env))

(defmethod qualify-clause :right-join [_ joins quoting env]
 (qualify-join joins quoting env))

;; TODO: traverse into SqlCalls?
(defn qualify-query
  "Qualifies and quotes all identifiers in a query, taking into account entity
  and field name mappings"
  ([q]
    (qualify-query q nil))
  ([q quoting]
    (qualify-query q quoting {}))
  ([q quoting env]
    (reduce-kv
      (fn [q clause cval]
        (let [cval* (qualify-clause clause cval quoting env)]
          (if (and (not (nil? cval))
                   (or (not (coll? cval*))
                       (seq cval*)))
            (assoc q clause cval*)
            q)))
      q q)))

(defn plain-sql?
  "Returns true if q is a plain SQL string or clojure.java.jdbc-style
  [sql params] vector"
  [q]
  (or (string? q)
      (and (vector? q) (string? (first q)))
      (and (sequential? q) (string? (first q)))))

(defn to-sql
  "Transforms a Cantata query into a clojure.java.jdbc-compatible [sql params]
  vector, leveraging the provided data model and other information"
  [q & {:keys [data-model quoting expanded env params]}]
  (cond
   (string? q) [q]
   (and (vector? q) (string? (first q))) q
   (and (sequential? q) (string? (first q))) (vec q)
   :else (let [[eq env] (if (or expanded (not data-model))
                          [q env]
                          (cq/expand-query data-model q))
               qq (qualify-query eq quoting (or env {}))]
           (hq/format qq
                      :quoting quoting
                      :params params))))

(defn prepared?
  "Returns true if q is a PreparedQuery instance"
  [q]
  (instance? PreparedQuery q))

;; TODO: prepared statements; need a way to manage open/close scope;
;; maybe piggyback on transaction scope?
(defn prepare
  "See cantata.core/prepare-query"
  [ds dm q & {:keys [expanded env force-pk] :as opts}]
  (let [[eq env added-paths] (if (or expanded (plain-sql? q))
                               [q env]
                               (cq/expand-query dm q :force-pk (not (false? force-pk))))
        [sql] (apply to-sql eq
                     :data-model dm
                     :quoting (cds/get-quoting ds)
                     :expanded true
                     :env env
                     (apply concat opts))
        param-names (map cq/param-name (filter cq/param? (keys env)))]
    (r/->PreparedQuery eq env sql param-names added-paths)))

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

(defn query
  "Implementation of cantata.core/query"
  [ds dm q callback & {:keys [expanded env params force-pk]}]
  (let [prepped? (prepared? q)
        [eq env added-paths] (cond
                               prepped? [(:expanded-query q) (:env q) (:added-paths q)]
                               (or expanded (plain-sql? q)) [q env]
                               :else (cq/expand-query dm q :force-pk force-pk))
        [sql & sql-params] (if prepped?
                             (into [(:sql q)] (map #(get params %) (:param-names q)))
                             (to-sql eq
                                     :data-model dm
                                     :quoting (cds/get-quoting ds)
                                     :expanded true
                                     :env env
                                     :params params))
        marshaller (cds/get-marshaller ds)
        jdbc-q (into [sql] (map marshaller sql-params))
        _ (when cu/*verbose* (prn jdbc-q))
        from-ent (cdm/entity dm (:from eq))
        row-fn (get-row-fn ds eq from-ent env)
        [cols & rows] (jd/query ds jdbc-q
                                :identifiers dasherize
                                :row-fn row-fn
                                :as-arrays? true)
        qmeta {:cantata.core/query {:from from-ent
                                    :env env
                                    :expanded eq
                                    :added-paths added-paths}}]
    (callback (with-meta cols qmeta)
              rows)))

(defn query-count
  "Implementation of cantata.core/query-count"
  [ds dm q & {:keys [flat] :as opts}]
  (when (plain-sql? q)
    (throw-info "query-count not supported on plain SQL" {:q q}))
  (let [quoting (cds/get-quoting ds)
        [q env] (cq/expand-query dm q)
        ent (cdm/entity dm (:from q))
        select (if (or flat
                       (not-any? q [:join :left-join :right-join]))
                 [[(hq/call :count (hq/raw "*")) :cnt]]
                 (let [npk (cdm/normalize-pk (:pk ent))
                       qpk (for [pk npk]
                             (qualify
                               (cdm/resolve-path dm ent npk)
                               quoting))]
                   [[(apply hq/call :count-distinct qpk) :cnt]]))
        q (-> q
            (dissoc :limit :offset)
            (assoc :select select))]
    (apply query ds dm q #(ffirst %2)
           :expanded true
           :env env
           (apply concat opts))))

(defn add-limit-1
  "Adds a \"limit 1\" clause to query q, which can be a Cantata query or plain
  SQL string"
  [q]
  (if (prepared? q)
    q
    (let [q (if (or (map? q) (string? q)) [q] q)
        qargs1 (first q)]
      (if (string? qargs1) ;support plain SQL
        (if (re-find #"(?i)\blimit\s+\d+" qargs1)
          q
          (cons (str qargs1 " LIMIT 1") (rest q)))
        (concat q [:limit 1])))))

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
                (qualify-pred-fields quoting env)
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
                               (qualify-pred-fields quoting env)
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