(ns cantata.sql
  (:require [cantata.data-model :as cdm]
            [cantata.query :as cq]
            [cantata.util :as cu]
            [clojure.java.jdbc :as jd]
            [honeysql.core :as hq]
            [clojure.string :as string])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(defmulti qualify (fn [x]
                    (-> x :resolved :type)))

(defmethod qualify :default [x]
  x)

(defmethod qualify :field [x]
  ;; TODO: chain qualifiers
  (keyword (-> x :resolved :value :db-name)))

(defn finalize-q [dm qargs]
  (let [{:keys [q fields resolved-fields]} (cq/prep-query dm qargs)
        ent (cdm/entity dm (:from q))
        _ (when-not ent 
            (throw (ex-info (str "No such entity - " (:from q))
                            {:q q :data-model dm})))
        ;; FIXME: identifier honeysql type
        from [[(keyword (:db-name ent)) (:name ent)]]
        select (for [field (:select q)]
                 ;; TODO: respect explicit aliases
                 (if-let [rp (resolved-fields field)]
                   [(qualify rp) field]
                   field))]
    ;; TODO db names, aliasing
    (assoc q
           :from from
           :select select)))

(def subprotocol->quoting
  {"postgresql" :ansi
   "postgres" :ansi
   "h2" :ansi
   "derby" :ansi
   "hsqldb" :ansi
   "sqlite" :ansi
   "mysql" :mysql
   "sqlserver" :sqlserver
   "jtds:sqlserver" :sqlserver})

(defn detect-quoting [ds]
  (if-let [subprot (:subprotocol ds)]
    (subprotocol->quoting subprot)
    (let [ds* (:datasource ds)]
      (when (and ds* (instance? ComboPooledDataSource ds*))
        (let [url (.getJdbcUrl ^ComboPooledDataSource ds*)]
          (when-let [subprot (second (re-find #"^([^:]+):"
                                              (string/replace url #"^jdbc:" "")))]
            (subprotocol->quoting subprot)))))))

(defn to-sql [ds dm q]
  (cond
   (string? q) [q]
   (and (vector? q) (string? (first q))) q
   (and (sequential? q) (string? (first q))) (vec q)
   :else (hq/format
           (finalize-q dm q) :quoting (or (:cantata.core/quoting ds)
                                          (detect-quoting ds)))))

(defn dasherize [s]
  (string/replace s "_" "-"))

(defn undasherize [s]
  (string/replace s "-" "_"))

(defn query [ds dm q callback]
  (let [sql-params (to-sql ds dm q)
        _ (when cu/*verbose* (prn sql-params))
        [cols & rows] (jd/query ds sql-params
                                :identifiers dasherize
                                :as-arrays? true)]
    (callback cols rows)))

(defn query-count [ds dm q callback])
(defn save [ds dm ename changes opts])
(defn delete [ds dm ename pred opts])

;; TODO: more customizable options
(defn create-pool [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec)) 
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               ;; expire excess connections after 30 minutes of inactivity:
               (.setMaxIdleTimeExcessConnections (* 30 60))
               ;; expire connections after 3 hours of inactivity:
               (.setMaxIdleTime (* 3 60 60)))] 
    {:datasource cpds}))