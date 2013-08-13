(ns cantata.data-source
  (:require [cantata.data-model :as cdm]
            [cantata.util :refer [throw-info]]
            [clojure.java.jdbc :as jd]
            [clojure.string :as string])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(set! *warn-on-reflection* true)

(defn normalize-db-spec [db-spec]
  (cond
    (map? db-spec) db-spec
    (string? db-spec) {:connection-uri db-spec}
    (instance? java.net.URI db-spec) (normalize-db-spec (str db-spec))
    :else (throw-info "Unrecognized db-spec format" {:db-spec db-spec})))

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

(defn detect-quoting [db-spec]
  (if-let [subprot (:subprotocol db-spec)]
    (subprotocol->quoting subprot)
    (let [ds (:datasource db-spec)]
      (when (and ds (instance? ComboPooledDataSource ds))
        (let [url (.getJdbcUrl ^ComboPooledDataSource ds)]
          (when-let [subprot (second (re-find #"^([^:]+):"
                                              (string/replace url #"^jdbc:" "")))]
            (subprotocol->quoting subprot)))))))

;; TODO: more customizable options
(defn create-pool [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec))
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               (.setMaxIdleTimeExcessConnections (:max-idle-excess-connections
                                                   spec (* 30 60)))
               (.setMaxIdleTime (:max-idle spec (* 3 60 60))))]
    {:datasource cpds}))

(defn data-source [db-spec & model+opts]
  (let [arg1 (first model+opts)
        [dm opts] (if (keyword? arg1)
                    [nil model+opts]
                    [arg1 (rest model+opts)])
        opts (apply hash-map opts)
        ds (normalize-db-spec db-spec)
        ds (if (:pooled opts)
             (create-pool db-spec (merge db-spec opts))
             db-spec)
        dm (if (:reflect opts)
             (cdm/reflect-data-model ds dm)
             (when dm
               (if (cdm/data-model? dm)
                 dm
                 (cdm/data-model dm))))
        ds (assoc ds ::quoting (if (contains? opts :quoting)
                                 (:quoting opts)
                                 (detect-quoting ds)))]
    (cond-> ds
            dm (assoc ::data-model dm))))

(defn pooled-data-source
  ;; TODO: doc opts & defaults
  [db-spec & model+opts]
  (let [opts (apply hash-map (if (keyword? (first model+opts))
                               model+opts
                               (rest model+opts)))]
    (apply data-source
           (create-pool (merge (normalize-db-spec db-spec)
                               opts))
           model+opts)))

(defn get-data-model [ds]
  (::data-model (force ds)))

(defn get-quoting [ds]
  (::quoting (force ds)))

