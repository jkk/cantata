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
  (apply cds/data-source db-spec model+opts))

(defn pooled-data-source [db-spec & model+opts]
  (apply cds/pooled-data-source db-spec model+opts))

(defn build-query [& qargs]
  (apply cq/build-query qargs))

(defn merge-query [q & qargs]
  (apply cq/build-query q qargs))

(defn with-query-rows*
  ([ds q body-fn]
    (with-query-rows* ds q nil body-fn))
  ([ds q opts body-fn]
    (apply sql/query (force ds) (cds/get-data-model ds) q body-fn
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
    (apply sql/query (force ds) (cds/get-data-model ds) q
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

(defn get-query-expanded [x]
  (::query-expanded (meta x)))

;; TODO: version that works on arbitrary vector/map data, sans env
(defn nest
  ([cols rows]
    (with-meta
      (cq/nest cols rows (get-query-from cols) (get-query-env cols))
      (meta cols))))

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
        (with-meta
          (mapv #(cu/zip-ordered-map cols %) rows)
          (meta cols))))))

(defn flat-query1 [ds q & {:keys [vectors] :as opts}]
  (with-query-rows* ds (sql/add-limit-1 q) opts
    (fn [cols rows]
      (if vectors
        [cols (first rows)]
        (when (first rows)
          (with-meta
            (cu/zip-ordered-map cols (first rows))
            (meta cols)))))))

(defn query* [ds q & opts]
  (when (sql/plain-sql? q)
    (throw-info "Use flat-query to execute plain SQL queries" {:q q}))
  ;; TODO: implicitly add/remove PKs if necessary? :force-pk opt?
  (with-query-rows* ds q opts
    (fn [cols rows]
      (nest cols rows))))

(defn query1* [ds q & opts]
  (let [ms (apply query* ds (sql/add-limit-1 q) opts)]
    (when-let [m (first ms)]
      (with-meta m (meta ms)))))

(declare query)

(defn query1 [ds q & opts]
  (let [ms (apply query ds (sql/add-limit-1 q) opts)]
    (when-let [m (first ms)]
      (with-meta m (meta ms)))))

(defn ^:private build-by-id-query [ds ename id]
  (let [ent (cdm/entity (cds/get-data-model ds) ename)]
    {:from ename :where [:= id (:pk ent)]}))

(defn by-id [ds ename id & [q & opts]]
  (let [baseq (build-by-id-query ds ename id)]
    (apply query1 ds (if (map? q)
                       [baseq q]
                       (cons baseq q))
           opts)))

(defn by-id* [ds ename id & [q & opts]]
  (let [baseq (build-by-id-query ds ename id)
        ms (apply query* ds (if (map? q)
                              [baseq q]
                              (cons baseq q))
                  opts)]
    (when-let [m (first ms)]
      (with-meta m (meta ms)))))

(defn query-count [ds q & opts]
  (apply sql/query-count (force ds) (cds/get-data-model ds) q opts))

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

(defmacro with-debug
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
        dm (or dm (cds/get-data-model ds))]
    (apply cq/expand-query dm q opts)))

(defn to-sql [ds-or-dm q & opts]
  (let [[ds dm] (if (cdm/data-model? ds-or-dm)
                  [nil ds-or-dm]
                  [ds-or-dm nil])
        dm (or dm (cds/get-data-model ds))]
    (apply sql/to-sql q
           :data-model dm
           :quoting (when ds (cds/get-quoting ds))
           opts)))

(defn prepare [ds q & opts]
  (apply sql/prepare (force ds) (cds/get-data-model ds) q opts))

;;;;

(defmacro ^:private def-dm-helpers [& fn-names]
  `(do
     ~@(for [fn-name fn-names]
         `(defn ~fn-name [~'ds ~'& ~'args]
            (apply ~(symbol "cdm" (name fn-name))
                   (cds/get-data-model ~'ds) ~'args)))))

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

(defn queryf [ds q & opts]
  (let [res (apply flat-query ds q opts)
        env (get-query-env res)
        eq (get-query-expanded res)
        f1name (:final-path (env (first (:select eq))))]
    (map f1name res)))

(defn queryf1 [ds q & opts]
  (let [res (apply flat-query1 ds q opts)
        env (get-query-env res)
        eq (get-query-expanded res)
        f1name (:final-path (env (first (:select eq))))]
    (f1name res)))

;;;;

(defn ^:private fetch-one-maps [ds eq fields any-many? opts]
  (let [q (assoc eq
                 :select fields
                 :modifiers (when any-many? [:distinct]))]
    (apply query* ds q opts)))

(defn ^:private build-key-pred [pk id-or-ids]
  (if (and (sequential? pk) (< 1 (count pk)))
    (if (sequential? (first id-or-ids))
      (into [:or] (for [id id-or-ids]
                    (into [:and] (for [[pk id] (map list pk id)]
                                   [:= pk id]))))
      [:= pk id-or-ids])
    (let [pk (if (sequential? pk)
               (first pk)
               pk)]
      (if (and (sequential? id-or-ids) (< 1 (count id-or-ids)))
        [:in pk (vec id-or-ids)]
        [:= pk (if (sequential? id-or-ids)
                 (first id-or-ids)
                 id-or-ids)]))))

(defn ^:private fetch-many-results [ds ent pk npk ids many-groups opts]
  (for [[qual fields] many-groups]
    ;; TODO: can be done with fewer joins by using reverse rels
    (let [q {:select (concat (remove (set fields) npk)
                             fields)
             :from (:name ent)
             :where (build-key-pred pk ids)
             :modifiers [:distinct]
             :order-by pk}
          maps (apply query* ds q opts)]
      [qual (into {} (for [m maps]
                       [(cdm/pk-val m pk)
                        (getf m qual)]))])))

(defn ^:private incorporate-many-results [pk pk? npk maps many-results sks]
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
  (let [dm (cds/get-data-model ds)
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
        many-rel-names (set (map (comp cu/qualifier :final-path env)
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
                     pk pk? npk maps many-results select-paths)))]
    (with-meta
      maps
      {::query-from ent
       ::query-env env
       ::query-expanded eq}))))

;;;;

(defn ^:private get-own-map
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
        dm (cds/get-data-model ds)
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
  (let [dm (cds/get-data-model ds)
        ent (cdm/entity dm ename)
        values* (get-own-map ent values)]
    (sql/update! (force ds) dm ename values pred)))

(defn delete!
  "Deletes records from data source that match the given predicate, or all
  records if no predicate is provided.

  Returns the number of records deleted."
  ([ds ename]
    (delete! ds ename nil))
  ([ds ename pred & {:as opts}]
    (let [dm (cds/get-data-model ds)]
      (sql/delete! (force ds) dm ename pred))))

(defn cascading-delete!
  "Deletes ALL database records for an entity, and ALL dependent records. Or,
  if no entity is given, deletes ALL records for ALL entities in the data
  model. BE CAREFUL!"
  ([ds]
    (let [dm (cds/get-data-model ds)]
      ;; Doesn't keep track of which have already been deleted, so will
      ;; delete entities multiple times. Oh well.
      (doseq [ent (cdm/entities dm)]
        (cascading-delete! ds (:name ent)))))
  ([ds ename]
    (let [dm (cds/get-data-model ds)
          deps (cdm/dependent-graph dm)]
      (doseq [[dep-name] (deps ename)]
        (cascading-delete! ds dep-name))
      (delete! ds ename))))

(defn delete-ids!
  "Deletes records from data source that match the given ids.

  Returns the number of records deleted."
  [ds ename id-or-ids & {:as opts}]
  (let [dm (cds/get-data-model ds)
        ids (cu/seqify id-or-ids)
        ent (cdm/entity dm ename)]
    (sql/delete! (force ds) dm ename (build-key-pred (:pk ent) ids))))

(defn cascading-delete-ids!
  "Deletes any dependent records and then deletes the entity record for the
  given id"
  [ds ename id-or-ids]
  (let [dm (cds/get-data-model ds)
        ent (cdm/entity dm ename)
        pk (:pk ent)
        npk (cdm/normalize-pk pk)
        deps (cdm/dependent-graph dm)]
    (doseq [[dep-name dep-rel] (deps ename)]
      (let [dep-ent (cdm/entity dm dep-name)
            dep-pk (:pk dep-ent)
            other-rk (vec (for [k (cdm/normalize-pk (:other-key dep-rel))]
                            (cu/join-path (:name dep-rel) k)))
            other-pk (vec (for [k npk]
                            (cu/join-path (:name dep-rel) k)))
            del-ms (flat-query
                     ds [:from dep-name
                         :select (cdm/normalize-pk dep-pk)
                         :where [:and
                                 (build-key-pred (:key dep-rel) other-rk)
                                 (build-key-pred other-pk id-or-ids)]])]
        (doseq [del-m del-ms]
          (cascading-delete-ids! ds dep-name (cdm/pk-val del-m dep-pk))))))
  (delete-ids! ds ename id-or-ids))

;;;;

(declare save!)

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

(defn ^:private assoc-pk [m pk id]
  (if (sequential? pk)
    (reduce
      (fn [m [pk id]]
        (assoc m pk id))
      (map list pk id))
    (assoc m pk id)))

(defn ^:private save-belongs-to [ds dm rel rms ent m]
  (let [rent (cdm/entity dm (:ename rel))
        rm (first rms) ;one-to-one
        rpk (:pk rent)
        nrpk (cdm/normalize-pk rpk)
        rid (if (and (= (count nrpk) (count rm))
                     (cdm/pk-present? rm rpk))
              (cdm/pk-val rm rpk)
              (save! ds (:name rent) rm))
        pk (:pk ent)]
    (assoc-pk m (:key rel) rid)))

(defn ^:private save-has-many-via [ds dm rels rms ent id]
  (let [[via-rel rel] rels
        vent (cdm/entity dm (:ename via-rel))
        rent (cdm/entity dm (:ename rel))
        vfk (:other-key via-rel)
        vrfk (:other-key rel)
        old-vms (flat-query
                  ds [:from (:name vent)
                      :select vrfk
                      :where (build-key-pred vfk id)])
        old-rids (map #(cdm/pk-val % vrfk) old-vms)
        rids (doall
              (for [rm rms]
                (save! ds (:name rent) rm)))
        rem-rids (remove (set rids) old-rids)
        add-rids (remove (set old-rids) rids)]
    (doseq [rid add-rids]
      (save! ds (:name vent) (-> {}
                               (assoc-pk vfk id)
                               (assoc-pk vrfk rid))))
    ;; Only recs in the junction table are removed; NOT from the rel table
    (when (seq rem-rids)
      (delete! ds (:name vent) (build-key-pred
                                 [vfk vrfk]
                                 (map vector (repeat id) rem-rids))))
    true))


(defn ^:private save-has-many [ds dm rels rms ent id]
  (if (< 1 (count rels))
    (save-has-many-via ds dm rels rms ent id)
    (let [rel (first rels)
          rent (cdm/entity dm (:ename rel))
          rpk (:pk rent)
          fk (or (:other-key rel) rpk)
          rename (:name rent)
          old-rms (flat-query
                    ds [:from rename :select rpk :where (build-key-pred fk id)])
          rids (doall
                (for [rm rms]
                  (save! ds (:name rent) (assoc-pk rm fk id))))
          rem-rids (remove (set rids)
                           (map #(cdm/pk-val % rpk) old-rms))]
      (when (seq rem-rids)
        (delete-ids! ds rename rem-rids))
      true)))

(defn ^:private rel-save-allowed? [chain]
  ;; Must be a belongs-to or has-many relationship
  (or (= 1 (count chain))
      (and (= 2 (count chain))
           (let [[l1 l2] chain]
             (and (-> l1 :rel :reverse)
                  (not (-> l2 :rel :reverse)))))))

(defn ^:private get-rel-maps [dm ent m]
  (let [rels (cdm/rels ent)]
    (reduce-kv
      (fn [rms k v]
        (let [rp (cdm/resolve-path dm ent k)
              chain (not-empty (:chain rp))]
          (if-not chain
            rms
            (cond
              (not (rel-save-allowed? chain))
              (throw-info
                ["Rel" k "not allowed here - saving entity" (:name ent)]
                {:rname k :ename (:name ent)})
              (not (sequential? v))
              (throw-info
                ["Rel value" k "must be sequential - saving entity" (:name ent)]
                {:rname k :ename (:name ent)})
              :else	(assoc rms (mapv :rel chain) v)))))
      {} m)))

(defn ^:private save-m-rels [ds dm ent own-m m]
  (let [rel-ms (get-rel-maps dm ent m)
        [fwd-rel-ms rev-rel-ms] ((juxt filter remove)
                                  #(-> % key first :reverse not)
                                  rel-ms)
        own-m (reduce
                (fn [own-m [[rel] rms]]
                  (save-belongs-to ds dm rel rms ent own-m))
                own-m
                fwd-rel-ms)
        id (save-m ds ent own-m)]
    (doseq [[rels rms] rev-rel-ms]
      (save-has-many ds dm rels rms ent id))
    id))

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
  (let [dm (cds/get-data-model ds)
        ent (cdm/entity dm ename)
        own-m (get-own-map ent values)]
    (with-transaction ds
      (if (false? (:save-rels opts))
        (save-m ds ent own-m)
        (save-m-rels ds dm ent own-m values)))))