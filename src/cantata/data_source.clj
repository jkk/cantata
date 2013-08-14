(ns cantata.data-source
  (:require [cantata.data-model :as cdm]
            [cantata.util :refer [throw-info]]
            [cantata.protocols :as cp]
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

(def ^:private unmarshal-fnmap
  {:clob-str cp/clob->str
   :blob-bytes cp/blob->bytes
   :joda-dates cp/date->joda})

(def ^:private marshal-fnmap
  {:joda-dates cp/joda->date})

(defn ^:private make-marshalling-fn [opts fnmap]
  (when-let [fs (seq (for [[opt v] opts
                           :let [f (fnmap opt)]
                           :when (and v f)]
                       f))]
    (apply comp fs)))

(defn data-source [db-spec & model+opts]
  (let [arg1 (first model+opts)
        [dm opts] (if (keyword? arg1)
                    [nil model+opts]
                    [arg1 (rest model+opts)])
        opts (apply hash-map opts)
        ds (normalize-db-spec db-spec)
        ds (if (:pooled opts)
             (create-pool (merge ds opts))
             db-spec)
        dm (if (:reflect opts)
             (apply cdm/reflect-data-model ds dm (apply concat opts))
             (when dm
               (if (cdm/data-model? dm)
                 dm
                 (cdm/data-model dm))))
        ds (assoc ds
                  ::options opts
                  ::quoting (if (contains? opts :quoting)
                                 (:quoting opts)
                                 (detect-quoting ds))
                  ::marshaller (make-marshalling-fn opts marshal-fnmap)
                  ::unmarshaller (make-marshalling-fn opts unmarshal-fnmap))]
    (cond-> ds
            dm (assoc ::data-model dm))))

(defn get-data-model [ds]
  (::data-model (force ds)))

(defn get-options [ds]
  (::options (force ds)))

(defn get-option [ds opt & [default]]
  (opt (::options (force ds)) default))

(defn get-quoting [ds]
  (::quoting (force ds)))

(defn get-marshaller [ds]
  (::marshaller (force ds)))

(defn get-unmarshaller [ds]
  (::unmarshaller (force ds)))

(defn get-row-unmarshaller [ds]
  (if-let [unmarshal (get-unmarshaller ds)]
    ;; Assumes most row values will not need to be marshalled, so merely
    ;; updates selective values with assoc.
    ;;
    ;; We could avoid this overhead if clojure.java.jdbc had a way
    ;; to inject a column value construction function. (Extending the
    ;; IResultSetReadColumn protocol would change the behavior for ALL
    ;; data sources instead of just this one, so it's not an
    ;; option.)
    (fn [row]
      (loop [i 0
             row row]
        (if (= i (count row))
          row
          (let [oldv (nth row i)
                newv (unmarshal oldv)]
            (recur (inc i) (if (identical? oldv newv)
                             row
                             (assoc row i newv)))))))
    identity))