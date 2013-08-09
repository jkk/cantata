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

(defn get-data-model [ds]
  (::data-model (force ds)))

(defn build-query [& qargs]
  (apply cq/build-query qargs))

(defn with-query-rows* [ds q body-fn]
  (sql/query (force ds) (get-data-model ds) q body-fn))

(defmacro with-query-rows [ds q cols rows & body]
  `(with-query-rows* ~ds ~q (fn [~cols ~rows]
                              ~@body)))

(defn query* [ds q]
  (with-query-rows ds q cols rows
    [cols rows]))

(defn query [ds q]
  (with-query-rows ds q cols rows
    (mapv #(cu/zip-ordered-map cols %) rows)))

(defn- add-limit-1 [q]
  (let [q (if (or (map? q) (string? q)) [q] q)
        qargs1 (first q)]
    (if (string? qargs1) ;support plain SQL
      (if (re-find #"(?i)\blimit\s+\d+" qargs1)
        q
        (cons (str qargs1 " LIMIT 1") (rest q)))
      (concat q [:limit 1]))))

(defn query1 [ds q]
  (first
    (query ds (add-limit-1 q))))

(defn by-id [ds ename id & [q]]
  (let [ent (cdm/entity (get-data-model ds) ename)
        baseq {:from ename :where [:= id (:pk ent)]}]
    (query1 ds (if (map? q)
                 [baseq q]
                 (cons baseq q)))))

(defn query-count [ds q]
  (sql/query-count (force ds) (get-data-model ds) q))

(defn query-count* [ds q]
  (sql/query-count (force ds) (get-data-model ds) q :flat true))

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

(defn to-sql [ds-or-dm q & opts]
  (let [[ds dm] (if (cdm/data-model? ds-or-dm)
                  [nil ds-or-dm]
                  [ds-or-dm nil])
        dm (or dm (get-data-model ds))]
    (apply sql/to-sql q
           :data-model dm
           :quoting (when ds (sql/detect-quoting
                               (force ds)))
           opts)))

;;;;

(defmacro ^:private def-dm-helpers [& fn-names]
  `(do
     ~@(for [fn-name fn-names]
         `(defn ~fn-name [~'ds ~'& ~'args]
            (apply ~(symbol "cdm" (name fn-name))
                   (get-data-model ~'ds) ~'args)))))

(def-dm-helpers
  entities entity rels rel fields field field-names shortcut shortcuts resolve-path)

