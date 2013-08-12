(ns cantata.core
  (:require [cantata.util :as cu :refer [throw-info]]
            [cantata.data-model :as cdm]
            [cantata.query :as cq]
            [cantata.sql :as sql]
            [clojure.java.jdbc :as jd]
            [flatland.ordered.map :as om]))

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

(defn ^:private normalize-db-spec [db-spec]
  (cond
    (map? db-spec) db-spec
    (string? db-spec) {:connection-uri db-spec}
    (instance? java.net.URI db-spec) (normalize-db-spec (str db-spec))
    :else (throw-info "Unrecognized db-spec format" {:db-spec db-spec})))

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

;; TODO: helpers from modelo - field, field1, col1, merge-where, merge-select

(defn get-query-env [x]
  (::query-env (meta x)))

(defn get-query-from [x]
  (::query-from (meta x)))

;; TODO: version that works on arbitrary vector/map data, sans env
(defn nest
  ([cols rows]
    (cq/nest cols rows (get-query-from cols) (get-query-env cols))))

;;
;; QUERY CHOICES
;; * As nested maps, multiple queries - (query ...)
;; * As nested maps, single query     - (query* ...)
;; * As flat maps, single query       - (query* ... :flat true)
;; * As vectors, single query         - (query* ... :vectors true)
;;

(defn query* [ds q & {:keys [vectors flat]}]
  (with-query-rows ds q cols rows
    (cond
      vectors [cols rows]
      (or flat (sql/plain-sql? q)) (mapv #(cu/zip-ordered-map cols %) rows)
      ;; TODO: implicitly add/remove PKs if necessary? :force-pk opt?
      :else (nest cols rows))))

(defn query1* [ds q & opts]
  (let [ret (apply query* ds (sql/add-limit-1 q) opts)]
    (if (vector? (first ret))
      [(first ret) (first (second ret))]
      (first ret))))


(defn query [ds q]
  (with-query-rows ds q cols rows
    ;; TODO
    ))

(defn query1 [ds q]
  (first
    (query ds q)))

(defn by-id [ds ename id & [q]]
  (let [ent (cdm/entity (get-data-model ds) ename)
        baseq {:from ename :where [:= id (:pk ent)]}]
    (first
      ;; TODO: use query?
      (query* ds (if (map? q)
                 [baseq q]
                 (cons baseq q))))))

(defn query-count [ds q & {:keys [flat]}]
  (sql/query-count (force ds) (get-data-model ds) q :flat flat))

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

(defn prep-query [ds-or-dm q & opts]
  (let [[ds dm] (if (cdm/data-model? ds-or-dm)
                  [nil ds-or-dm]
                  [ds-or-dm nil])
        dm (or dm (get-data-model ds))]
    (apply cq/prep-query dm q opts)))

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

