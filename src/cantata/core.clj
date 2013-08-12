(ns cantata.core
  (:require [cantata.util :as cu :refer [throw-info]]
            [cantata.data-model :as cdm]
            [cantata.data-source :as cds]
            [cantata.query :as cq]
            [cantata.sql :as sql]
            [clojure.java.jdbc :as jd]
            [flatland.ordered.map :as om]))

(defn data-model [entity-specs]
  (cdm/data-model entity-specs))

(defn reflect-data-model [ds entity-specs]
  (cdm/reflect-data-model (force ds) entity-specs))

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
    (cond-> (cds/normalize-db-spec db-spec)
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

(defn merge-query [q & qargs]
  (apply cq/build-query q qargs))

(defn with-query-rows*
  ([ds q body-fn]
    (with-query-rows* ds q nil body-fn))
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
        (when (first rows)
          (cu/zip-ordered-map cols (first rows)))))))

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

(defmacro with-transaction [binding & body]
  (let [[ds-sym ds] (if (symbol? binding)
                      [binding binding]
                      binding)]
    `(jd/db-transaction [~ds-sym (force ~ds)] ~@body)))

(defmacro rollback! [ds]
  `(jd/db-set-rollback-only! ~ds))

(defmacro unset-rollback! [ds]
  `(jd/db-unset-rollback-only! ~ds))

(defmacro rollback? [ds]
  `(jd/db-is-rollback-only ~ds))

(defmacro with-rollback [binding & body]
  (let [[ds-sym ds] (if (symbol? binding)
                      [binding binding]
                      binding)]
    `(with-transaction [~ds-sym ~ds]
       (rollback! ~ds-sym)
       ~@body)))

(defmacro verbose
  "Causes any wrapped queries or statements to print lots of logging
  information during execution. See also 'debug'"
  [& body]
  `(binding [cu/*verbose* true]
     (with-redefs [jd/db-do-prepared sql/db-do-prepared-hook
                   jd/db-do-prepared-return-keys sql/db-do-prepared-return-keys-hook]
       ~@body)))

(defmacro debug
  "Starts a transaction which will be rolled back when it ends, and prints
  verbose information about all database activity.

  WARNING: The underlying data source must actually support rollbacks. If it
  doesn't, changes may be permanently committed."
  [binding & body]
  `(verbose
     (with-rollback ~binding
       ~@body)))

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

(defn getf
  "Fetches one or more nested field values from the given query result or
  results, traversing into related results as necessary.

  If invoked with one argument, returns a partial that looks up the path."
  ([path]
    #(getf % path))
  ([qr path]
    ;; TODO: allow sequential path -> maps getf over each
    (or
      (path qr)
      (let [ks (cu/split-path path)]
        (reduce
          (fn [maps k]
            (if (sequential? maps)
              (let [maps* (map k maps)]
                (if (sequential? (first maps*))
                  (apply concat maps*)
                  maps*))
              (k maps)))
          qr
          ks)))))

(defn getf1
  "Fetches a nested field value from the given query result or results,
  traversing into related results as necessary. Returns the same as getf except
  if the result would be a sequence, in which case it returns the first
  element.

  If invoked with one argument, returns a partial that looks up the path."
  ([path]
    #(getf1 % path))
  ([qr path]
    (let [ret (getf qr path)]
      (if (sequential? ret)
        (first ret)
        ret))))

;;;;

(defn ^:private fetch-one-maps [ds eq fields any-many? opts]
  (let [q (assoc eq
                 :select fields
                 :modifiers (when any-many? [:distinct]))]
    (apply query* ds q opts)))

(defn ^:private fetch-many-results [ds ent pk npk ids many-groups opts]
  (for [[qual fields] many-groups]
    ;; TODO: can be done with fewer joins by using reverse rels
    (let [q {:select (concat (remove (set fields) npk)
                             fields)
             :from (:name ent)
             :where [:in pk ids]
             :modifiers [:distinct]
             :order-by pk}
          maps (apply query* ds q opts)]
      [qual (into {} (for [m maps]
                       [(cdm/pk-val m pk)
                        (getf m qual)]))])))

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
                (cq/nest-in
                  (if pk? m (apply dissoc m rempk))
                  (cu/split-path qual)
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

;;;;

(defn get-own-map
  "Returns a map with only the fields of the entity itself - no related
  fields."
  [ent m & {:as opts}]
  (let [fnames (cdm/field-names ent)]
    (if (seq fnames)
      (select-keys m (cdm/field-names ent))
      (select-keys m (remove cu/first-qualifier (keys m))))))

(defn insert!
  "Inserts an entity map or maps into the data source. Unlike save, the
  maps may not include related entity maps.

  Returns the primary key(s) of the inserted maps."
  [ds ename m-or-ms & {:as opts}]
  (let [ms (cu/seqify m-or-ms)
        dm (get-data-model ds)
        ent (cdm/entity dm ename)
        ret-keys (sql/insert! (force ds) dm ename
                              (map #(get-own-map ent %) ms))]
    (if (sequential? m-or-ms)
       ret-keys
       (first ret-keys))))

(defn update!
  "Updates data source records that match the given predicate with the given
  values. Unlike save, no related field values can be included.

  Returns the number of records affected by the update."
  [ds ename values pred & {:as opts}]
  (let [dm (get-data-model ds)
        ent (cdm/entity dm ename)
        values* (get-own-map ent values)]
    (sql/update! (force ds) dm ename values pred)))

(defn delete!
  "Deletes records from data source that match the given predicate.

  Returns the number of records deleted."
  [ds ename pred & {:as opts}]
  (let [dm (get-data-model ds)]
    (sql/delete! (force ds) dm ename pred)))

(defn delete-by-id!
  "Deletes records from data source that match the given ids.

  Returns the number of records deleted."
  [ds ename id-or-ids & {:as opts}]
  (let [dm (get-data-model ds)
        ids (cu/seqify id-or-ids)
        ent (cdm/entity dm ename)]
    (sql/delete! (force ds) dm ename [:in (:pk ent) ids])))

;;;;

(defn ^:private save-m [ds ent m]
  (let [pk (:pk ent)
        id (cdm/pk-val m pk)
        ename (:name ent)]
    (if (nil? id)
      (insert! ds ename m)
      (do
        (if (flat-query1 ds [:from ename :select pk :where [:= pk id]])
          (update! ds ename m [:= pk id])
          (insert! ds ename m))
        id))))

(defn save!
  "Saves entity map values to the database. If the primary key is included in
  the map, the DB record will be updated if it exists. If it doesn't exist, or
  if no primary key is provided, a new record will be inserted.

  The map may include nested, related maps, which will in turn be saved and
  associated with the top-level record. All saves happen within a transaction.

  Returns the primary key of the updated or inserted record.

  Accepts the following options:
      :save-rels - when false, does not save related records"
  [ds ename values & {:as opts}]
  (let [dm (get-data-model ds)
        ent (cdm/entity dm ename)
        own-m (get-own-map entity values)]
    (with-transaction ds
      (save-m ds ent own-m)
      (when-not (false? (:save-rels opts))
        ))))