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

(defn with-query-rows*
  ([ds q body-fn]
    (with-query-rows ds q nil body-fn))
  ([ds q opts body-fn]
    (apply sql/query (force ds) (get-data-model ds) q body-fn
           (if (map? opts)
             (apply concat opts)
             opts))))

(defmacro with-query-rows [cols rows ds q & body]
  `(with-query-rows* ~ds ~q (fn [~cols ~rows]
                              ~@body)))

(defn with-query-maps*
  ([ds q body-fn]
    (with-query-maps* ds q nil body-fn))
  ([ds q opts body-fn]
    (apply sql/query (force ds) (get-data-model ds) q
           (fn [cols rows]
             (body-fn (map #(cu/zip-ordered-map cols %) rows)))
           (if (map? opts)
             (apply concat opts)
             opts))))

(defmacro with-query-maps [maps ds q & body]
  `(with-query-maps* ~ds ~q (fn [~maps]
                              ~@body)))

;; TODO: helpers from modelo - getf, getf1, getc, merge-where, merge-select

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
;; * As flat maps, single query       - (flat-query ... :flat true)
;; * As vectors, single query         - (flat-query ... :vectors true)
;;

(defn flat-query [ds q & {:keys [vectors] :as opts}]
  (with-query-rows* ds q opts
    (fn [cols rows]
      (if vectors
        [cols rows]
        (mapv #(cu/zip-ordered-map cols %) rows)))))

(defn flat-query1 [ds q & {:keys [vectors] :as opts}]
  (with-query-rows* ds (sql/add-limit-1 q) opts
    (fn [cols rows]
      (if vectors
        [cols (first rows)]
        (cu/zip-ordered-map cols (first rows))))))

(defn query* [ds q & opts]
  (when (sql/plain-sql? q)
    (throw-info "Use flat-query to execute plain SQL queries" {:q q}))
  ;; TODO: implicitly add/remove PKs if necessary? :force-pk opt?
  (with-query-rows* ds q opts
    (fn [cols rows]
      (nest cols rows))))

(defn query1* [ds q & opts]
  (first
    (apply query* ds (sql/add-limit-1 q) opts)))

(declare query)

(defn query1 [ds q & opts]
  (first
    (apply query ds (sql/add-limit-1 q) opts)))

(defn ^:private build-by-id-query [ds ename id]
  (let [ent (cdm/entity (get-data-model ds) ename)]
    {:from ename :where [:= id (:pk ent)]}))

(defn by-id [ds ename id & [q & opts]]
  (let [baseq (build-by-id-query ds ename id)]
    (apply query1 ds (if (map? q)
                       [baseq q]
                       (cons baseq q))
           opts)))

(defn by-id* [ds ename id & [q & opts]]
  (let [baseq (build-by-id-query ds ename id)]
    (first
      (apply query* ds (if (map? q)
                         [baseq q]
                         (cons baseq q))
             opts))))

(defn query-count [ds q & opts]
  (apply sql/query-count (force ds) (get-data-model ds) q opts))

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

(defn expand-query [ds-or-dm q & opts]
  (let [[ds dm] (if (cdm/data-model? ds-or-dm)
                  [nil ds-or-dm]
                  [ds-or-dm nil])
        dm (or dm (get-data-model ds))]
    (apply cq/expand-query dm q opts)))

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

(defn prepare [ds q & opts]
  (apply sql/prepare (force ds) (get-data-model ds) q opts))

;;;;

(defmacro ^:private def-dm-helpers [& fn-names]
  `(do
     ~@(for [fn-name fn-names]
         `(defn ~fn-name [~'ds ~'& ~'args]
            (apply ~(symbol "cdm" (name fn-name))
                   (get-data-model ~'ds) ~'args)))))

(def-dm-helpers
  entities entity rels rel fields field field-names shortcut shortcuts resolve-path)

;;;;

(defn ^:private fetch-one-maps [ds eq fields any-many? opts]
  (let [q (assoc eq
                 :select fields
                 :modifiers (when any-many? [:distinct]))]
    (apply query* ds q opts)))

(defn ^:private fetch-many-results [ds ent pk npk ids many-groups opts]
  (for [[qual fields] many-groups]
    (let [q {:select (concat (remove (set fields) npk)
                             fields)
             :from (:name ent)
             :where [:in pk ids]
             :modifiers [:distinct]
             :order-by pk}
          maps (apply query* ds q opts)]
      [qual (into {} (for [m maps]
                       [(cdm/pk-val m pk)
                        (m qual)]))])))

(defn ^:private incorporate-many-results [pk pk? npk maps many-results env sks]
  (let [rempk (remove (set sks) npk)]
    (into
      []
      (if (empty? many-results)
        (if pk?
          maps
          (mapv #(apply dissoc % rempk) maps))
        (for [m maps]
          (reduce
            (fn [m [qual pk->rel-maps]]
              (let [id (cdm/pk-val m pk)
                    rel-maps (pk->rel-maps id)]
                (assoc
                  (if pk? m (apply dissoc m rempk))
                  qual
                  rel-maps)))
            m many-results))))))

(defn query [ds q & opts]
  (when (sql/plain-sql? q)
    (throw-info "Use flat-query to execute plain SQL queries" {:q q}))
  (when (sql/prepared? q)
    (throw-info "Use query* or flat-query to execute prepared queries" {:q q}))
  (let [dm (get-data-model ds)
        [eq env] (cq/expand-query dm q :expand-joins false)
        ent (-> (env (:from eq)) :resolved :value)
        pk (:pk ent)
        all-fields (filter #(= :field (-> % env :resolved :type))
                           (keys env))
        has-many? (comp #(some (comp :reverse :rel) %) :chain env)
        any-many? (boolean (seq (filter has-many? all-fields)))
        [many-fields one-fields] ((juxt filter remove)
                                  has-many?
                                  (:select eq))
        many-rel-names (set (map cu/qualifier
                                 many-fields))
        select-paths (map (comp :final-path env) (:select eq))
        [many-fields one-fields] ((juxt filter remove)
                                   (comp many-rel-names cu/qualifier)
                                   select-paths)
        pk? (cdm/pk-present? select-paths pk)
        npk (cdm/normalize-pk pk)
        one-fields (if pk?
                     one-fields
                     (concat (remove (set one-fields) npk)
                             one-fields))]
    ;; TODO: provide way to specify :group-by
    (when (and (seq many-fields)
              (or (:group-by eq)
                  (some #(= :agg-op (-> % env :resolved :type))
                        many-fields)))
     (throw-info
       "Multi-queries do not support aggregates or group-by for to-many-related fields"
       {:q q}))
    (let [maps (fetch-one-maps ds eq one-fields any-many? opts)
          maps (if-not any-many?
                 (if pk?
                   maps
                   (let [rempk (remove (set select-paths) npk)]
                     (mapv #(apply dissoc % rempk) maps)))
                 (let [ids (mapv #(cdm/pk-val % pk) maps)
                       many-groups (group-by cu/qualifier
                                             many-fields)
                       many-results (when (seq maps)
                                      (fetch-many-results
                                        ds ent pk npk ids many-groups opts))]
                   (incorporate-many-results
                     pk pk? npk maps many-results env select-paths)))]
    (with-meta
      maps
      {::query-from ent
       ::query-env env
       ::query-select (:select eq)}))))

