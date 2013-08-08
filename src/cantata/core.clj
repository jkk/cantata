(ns cantata.core
  (:require [cantata.util :as cu]
            [cantata.data-model :as cdm]
            [cantata.query :as cq]
            [cantata.sql :as sql]
            [clojure.java.jdbc :as jd]))

(defmacro verbose
  "Causes any wrapped queries or statements to print lots of logging
  information during execution. See also 'debug'"
  [& body]
  `(binding [cu/*verbose* true]
     ~@body))

(defn data-model [entity-specs]
  (cdm/data-model entity-specs))

(defn reflect-data-model [ds entity-specs]
  (cdm/reflect-data-model (force ds) entity-specs))

(defn- normalize-db-spec [db-spec]
  (cond
    (map? db-spec) db-spec
    (string? db-spec) {:connection-uri db-spec}
    (instance? java.net.URI db-spec) (normalize-db-spec (str db-spec))
    :else (throw (ex-info "Unrecognized db-spec format" {:db-spec db-spec}))))

(defn data-source [db-spec & model+opts]
  (let [arg1 (first model+opts)
        [dm opts] (if (keyword? arg1)
                    [nil model+opts]
                    [arg1 (rest model+opts)])
        opts (apply hash-map opts)
        dm (if (:reflect opts)
             (reflect-data-model db-spec dm)
             (when dm
               (if (cdm/data-model? dm)
                 dm
                 (data-model dm))))]
    (cond-> (normalize-db-spec db-spec)
            dm (assoc ::data-model dm)
            (contains? opts :quoting) (assoc ::quoting (:quoting opts)))))

(defn pooled-data-source [db-spec & model+opts]
  (apply data-source
         (sql/create-pool (normalize-db-spec db-spec))
         model+opts))

(defn build-query [& qargs]
  (apply cq/build-query qargs))

(defn with-query-rows*
  ([ds qargs body-fn]
    (with-query-rows* ds (::data-model (force ds)) qargs body-fn))
  ([ds dm qargs body-fn]
    (sql/query (force ds) dm qargs body-fn)))

;; supporting optional dm arg makes this really awkward
#_(defmacro with-query-rows
   {:arglists '([ds q cols rows & body] [ds dm q cols rows & body])}
   [ds & args]
   (let [[dm q cols rows & body] (if (symbol? (nth args 3))
                                   (cons `(::data-model ~ds) args)
                                   args)]
     `(with-query-rows* ~ds ~dm ~q
       (fn [~cols ~rows]
         ~@body))))

(defn ^:private get-dm+args [ds args]
  (if (cdm/data-model? (first args))
    [(first args) (rest args)]
    [(::data-model (force ds)) args]))

(defn query*
  [ds & qargs]
  (let [[dm qargs] (get-dm+args ds qargs)]
    (with-query-rows* ds dm qargs
      (fn [cols rows]
        [cols rows]))))

(defn query
  [ds & qargs]
  (let [[dm qargs] (get-dm+args ds qargs)]
    (with-query-rows* ds dm qargs
      (fn [cols rows]
        (mapv #(cu/zip-ordered-map cols %) rows)))))

(defn- add-limit-1 [qargs]
  (let [qargs1 (first qargs)]
    (if (string? qargs1) ;support plain SQL
      (if (re-find #"(?i)\blimit\s+\d+" qargs1)
        qargs
        (cons (str qargs1 " LIMIT 1") (rest qargs)))
      (concat qargs [:limit 1]))))

(defn query1 [ds & qargs]
  (first
    (apply query ds (add-limit-1 qargs))))

(defn by-id [ds & ename+id+args]
  (let [[dm [ename id & qargs]] (get-dm+args ds ename+id+args)]
    (let [ent (cdm/entity dm ename)]
      (apply query1 ds
             {:from ename :where [:= id (:pk ent)]}
             qargs))))

(defn query-count [ds & qargs]
  (let [[dm qargs] (get-dm+args ds qargs)]
    (sql/query-count (force ds) dm qargs)))

(defn query-count* [ds & qargs]
  (let [[dm qargs] (get-dm+args ds qargs)]
    (sql/query-count (force ds) dm qargs :flat true)))

(defn save! [ds dm ename changes opts])

(defn delete! [ds dm ename pred opts])

(defmacro transaction [[ds-sym ds] & body]
  `(jd/db-transaction [~ds-sym (force ~ds)] ~@body))

(defmacro rollback! [ds]
  `(jd/db-set-rollback-only! ~ds))

(defmacro unset-rollback! [ds]
  `(jd/db-unset-rollback-only! ~ds))

(defmacro rollback? [ds]
  `(jd/db-is-rollback-only ~ds))

(defn to-sql [ds-or-dm & qargs]
  (let [[ds dm] (if (cdm/data-model? ds-or-dm)
                  [nil ds-or-dm]
                  [ds-or-dm nil])
        dm (or dm
               (::data-model (force ds)))]
    (sql/to-sql dm qargs :quoting (when ds (sql/detect-quoting
                                             (force ds))))))

;;;;

(defmacro ^:private def-dm-helpers [& fn-names]
  `(do
     ~@(for [fn-name fn-names]
         `(defn ~fn-name [~'ds ~'& ~'args]
            (let [[dm# args#] (get-dm+args ~'ds ~'args)]
              (apply ~(symbol "cdm" (name fn-name)) dm# args#))))))

(def-dm-helpers
  entities entity rels rel fields field field-names shortcut shortcuts resolve-path)

