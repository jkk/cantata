(ns cantata.data-source
  "Implementation of data source related features

  See cantata.core for the main API entry point"
  (:require [cantata.data-model :as cdm]
            [cantata.util :refer [throw-info]]
            [cantata.protocols :as cp]
            [clojure.java.jdbc :as jd]
            [clojure.string :as string])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(set! *warn-on-reflection* true)

(defn ^:private get-url-subprotocol [url]
  (second (re-find #"^([^:]+):"
                   (string/replace url #"^jdbc:" ""))))

(defn normalize-db-spec
  "Takes a map, string, or URI and returns a clojure.java.jdbc-compatible
  db spec with :subprotocol set, amongst other keys provided"
  [db-spec]
  (cond
    (map? db-spec) db-spec
    (string? db-spec) {:subprotocol (get-url-subprotocol db-spec)
                       :connection-uri db-spec}
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

(defn get-subprotocol
  "Returns the subprotocol of a clojure.java.jdbc-compatible db spec"
  [db-spec]
  (or (:subprotocol db-spec)
      (when-let [url (:connection-uri db-spec)]
        (get-url-subprotocol url))
      (let [ds (:datasource db-spec)]
        (when (and ds (instance? ComboPooledDataSource ds))
          (get-url-subprotocol
            (.getJdbcUrl ^ComboPooledDataSource ds))))))

(defn detect-quoting
  "Guesses the quoting style of the given db spec"
  [db-spec]
  (when-let [subprot (get-subprotocol db-spec)]
    (subprotocol->quoting subprot)))

;; TODO: more customizable options
(defn create-pool
  "Creates a connection pool and returns a clojure.java.jdbc-compatble db spec
  for it"
  [spec]
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
  {:joda-dates cp/joda->date
   :marshal-dates cp/joda->date})

(defn ^:private make-marshalling-fn [opts fnmap]
  (when-let [fs (seq (for [[opt v] opts
                           :let [f (fnmap opt)]
                           :when (and v f)]
                       f))]
    (apply comp fs)))

(defn data-source
  "Implementation of cantata.core/data-source; see that function for docs"
  [db-spec & model+opts]
  (let [arg1 (first model+opts)
        [dm opts] (if (keyword? arg1)
                    [nil model+opts]
                    [arg1 (rest model+opts)])
        opts (assoc (apply hash-map opts)
                    :marshal-dates true)
        ds (normalize-db-spec db-spec)
        ds (if (:pooled opts)
             (create-pool (merge ds opts))
             ds)
        ds (assoc ds
                  ::options opts
                  ::quoting (if (contains? opts :quoting)
                              (:quoting opts)
                              (detect-quoting ds))
                  ::hooks (:hooks opts)
                  ::marshaller (make-marshalling-fn opts marshal-fnmap)
                  ::unmarshaller (make-marshalling-fn opts unmarshal-fnmap))]
    (when-let [init (:init-fn opts)]
      (init ds))
    (let [dm (if (:reflect opts)
               (apply cdm/reflect-data-model ds dm (apply concat opts))
               (when dm
                 (if (cdm/data-model? dm)
                   dm
                   (cdm/make-data-model dm))))]
      (assoc ds ::data-model dm))))

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

(defmulti ^:private invoke-nested-hook (fn [hname hf ent ret args]
                                         hname))

(defmethod invoke-nested-hook :default [hname hf ent ret _]
  (apply hf ent ret))

(defmethod invoke-nested-hook :after-query [hname hf ent ret _]
  (hf ent ret))

(defmethod invoke-nested-hook :validate [hname hf ent ret args]
  (concat ret (apply hf ent args)))

(defmethod invoke-nested-hook :before-save [hname hf ent ret _]
  (hf ent ret))

(defmethod invoke-nested-hook :after-save [hname hf ent ret [map]]
  (hf ent map ret))

(defmethod invoke-nested-hook :before-insert [hname hf ent ret _]
  (hf ent ret))

(defmethod invoke-nested-hook :after-insert [hname hf ent ret [maps]]
  (hf ent maps ret))

(defmethod invoke-nested-hook :after-update [hname hf ent ret [map pred]]
  (hf ent map pred ret))

(defmethod invoke-nested-hook :before-delete [hname hf ent ret _]
  (hf ent ret))

(defmethod invoke-nested-hook :after-delete [hname hf ent ret [pred]]
  (hf ent pred ret))

(defn maybe-invoke-hook
  [default ds ent hname & args]
  (if-let [ds-hf (get-in (force ds) [::hooks hname])]
    (let [ret (apply ds-hf ent args)]
      (if-let [hf (cdm/hook ent hname)]
        (invoke-nested-hook hname hf ent ret args)
        ret))
    (apply cdm/maybe-invoke-hook default ent hname args)))