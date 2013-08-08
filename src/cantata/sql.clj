(ns cantata.sql
  (:require [cantata.data-model :as cdm]
            [cantata.query :as cq]
            [cantata.util :as cu]
            [clojure.java.jdbc :as jd]
            [honeysql.core :as hq]
            [clojure.string :as string])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(defn identifier
  ([x quoting]
    (hq/raw (hq/quote-identifier x :style quoting :split false)))
  ([x y quoting]
    (if-not x
      (identifier y quoting)
      (hq/raw (str (hq/quote-identifier x :style quoting :split false)
                   "."
                   (hq/quote-identifier y :style quoting :split false))))))

(defmulti qualify (fn [x quoting]
                    (-> x :resolved :type)))

(defmethod qualify :default [x quoting]
  x)

(defmethod qualify :entity [x quoting]
  (let [ent (-> x :resolved :value)]
    (identifier (:db-schema ent) (:db-name ent) quoting)))

(defmethod qualify :field [x quoting]
  (let [fname (-> x :resolved :value :db-name)
        chain (:chain x)]
    (if (seq chain)
      (identifier (-> chain peek :to-path) fname quoting)
      (identifier (-> x :root :name) fname quoting))))

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

(def ^:dynamic *subquery-depth* -1)

(defmulti qualify-clause
  (fn [clause clause-val data-model quoting resolved-paths]
    clause))

(defmethod qualify-clause :default [_ cval _ _ _]
  cval)

(defmethod qualify-clause :from [_ from dm quoting _]
  (if-let [ent (cdm/entity dm from)]
    [[(identifier (:db-name ent) quoting) from]]
    (throw (ex-info (str "Unrecognized entity name in :from - " from)
                    {:from from}))))

;; TODO: subqueries
(defmethod qualify-clause :select [_ select _ quoting rps]
  (for [field select]
    ;; TODO: respect explicit aliases
    (if-let [rp (rps field)]
      [(qualify rp quoting) (identifier field quoting)]
      field)))

;; TODO: subqueries
(defn qualify-pred-fields [pred quoting rps]
  (when pred
    (let [fields (cq/get-predicate-fields pred)
          smap (into {} (for [f fields]
                          [f (qualify (rps f) quoting)]) )]
      (cq/replace-predicate-fields pred smap))))

(defmethod qualify-clause :where [_ where _ quoting rps]
 (qualify-pred-fields where quoting rps))

(defmethod qualify-clause :having [_ where _ quoting rps]
 (qualify-pred-fields where quoting rps))

(defmethod qualify-clause :order-by [_ order-by _ quoting rps]
 (when order-by
   (for [f order-by]
     (let [fname (if (coll? f) (first f) f)
           dir (when (coll? f) (second f))
           qfield (qualify (rps fname) quoting)]
       (if dir
         [qfield dir]
         qfield)))))

(defmethod qualify-clause :group-by [_ group-by _ quoting rps]
 (when group-by
   (for [fname group-by]
     (qualify (rps fname) quoting))))

;; TODO: subqueries
(defn- qualify-join [joins quoting rps]
 (mapcat (fn [[to on]]
           (let [ename (if (vector? to) (first to) to)
                 path (if (vector? to)
                        (second to)
                        ename)
                 qpath (identifier path quoting)
                 qename (qualify (rps path) quoting)
                 on* (qualify-pred-fields on quoting rps)]
             [[qename qpath] on*]))
         (partition 2 joins)))

(defmethod qualify-clause :join [_ joins _ quoting rps]
 (qualify-join joins quoting rps))

(defmethod qualify-clause :left-join [_ joins _ quoting rps]
 (qualify-join joins quoting rps))

(defmethod qualify-clause :right-join [_ joins _ quoting rps]
 (qualify-join joins quoting rps))

;; TODO: traverse into SqlCalls?
(defn qualify-query [dm quoting q resolved-paths]
  (binding [*subquery-depth* (inc *subquery-depth*)]
    (reduce-kv
      (fn [q clause cval]
        (let [cval* (qualify-clause clause cval dm quoting resolved-paths)]
          (if (and (not (nil? cval))
                   (or (not (coll? cval*))
                       (seq cval*)))
            (assoc q clause cval*)
            q)))
      q q)))

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
  (if (contains? ds :cantata.core/quoting)
    (:cantata.core/quoting ds)
    (if-let [subprot (:subprotocol ds)]
      (subprotocol->quoting subprot)
      (let [ds* (:datasource ds)]
        (when (and ds* (instance? ComboPooledDataSource ds*))
          (let [url (.getJdbcUrl ^ComboPooledDataSource ds*)]
            (when-let [subprot (second (re-find #"^([^:]+):"
                                                (string/replace url #"^jdbc:" "")))]
              (subprotocol->quoting subprot))))))))

(defn plain-sql? [q]
  (or (string? q)
      (and (vector? q) (string? (first q)))
      (and (sequential? q) (string? (first q)))))

(defn to-sql [ds dm q]
  (cond
   (string? q) [q]
   (and (vector? q) (string? (first q))) q
   (and (sequential? q) (string? (first q))) (vec q)
   :else (let [quoting (detect-quoting ds)
               {:keys [q resolved-paths]} (cq/prep-query dm q)]
           (hq/format
             (qualify-query dm quoting q resolved-paths)
             :quoting quoting))))

(defn dasherize [s]
  (string/replace s "[^^]_" "-"))

(defn undasherize [s]
  (string/replace s "-" "_"))

;;TODO: prepared queries/statements
(defn query [ds dm q callback]
  (let [sql-params (to-sql ds dm q)
        _ (when cu/*verbose* (prn sql-params))
        [cols & rows] (jd/query ds sql-params
                                :identifiers dasherize
                                :as-arrays? true)]
    (callback cols rows)))

(defn query-count [ds dm q & {:keys [flat]}]
  (when (plain-sql? q)
    (throw (ex-info "query-count not supported on plain SQL"
                    {:q q})))
  (let [quoting (detect-quoting ds)
        {:keys [q resolved-paths]} (cq/prep-query dm q)
        ent (cdm/entity dm (:from q))
        select (if (or flat
                       (not-any? q [:join :left-join :right-join]))
                 [(hq/call :count (hq/raw "*"))]
                 (let [npk (cdm/normalize-pk (:pk ent))
                       qpk (for [pk npk]
                             (qualify
                               (cdm/resolve-path dm ent npk)
                               quoting))]
                   [(apply hq/call :count-distinct qpk)]))
        q (-> q
            (dissoc :limit :offset)
            (assoc :select select))
        sql (hq/format (qualify-query dm quoting q resolved-paths)
                       :quoting quoting)]
    (query ds dm sql (fn [_ rows]
                       (ffirst rows)))))

(defn insert! [ds dm ename changes opts])
(defn update! [ds dm ename changes pred opts])
(defn delete! [ds dm ename pred opts])

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