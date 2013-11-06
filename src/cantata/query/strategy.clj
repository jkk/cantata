(ns cantata.query.strategy
  "Implementations of query fetching strategies"
  (:require [cantata.data-model :as dm]
            [cantata.query.build :as qb]
            [cantata.query.util :as qu]
            [cantata.query.expand :as qe]
            [cantata.util :as cu :refer [throw-info]]))

(defn ^:private fetch-one-maps [query-fn eq fields any-many? opts]
  (let [q (assoc eq
                 :select fields
                 :modifiers (when any-many? [:distinct]))]
    (apply query-fn q opts)))

(defn ^:private fetch-many-results
  [query-fn ent pk npk ids many-groups env opts]
  (for [[qual fields] many-groups]
    ;; TODO: can be done with fewer joins by using reverse rels
    (let [q {:select (concat (remove (set fields) npk)
                             fields)
             :from (:name ent)
             :where (qb/build-key-pred pk ids)}
          maps (apply query-fn q opts)
          qual-parts (cu/split-path qual)
          qual-revs (qu/get-qual-parts-reverses qual qual-parts env)]
      [qual (into {} (for [m maps]
                       [(dm/pk-val m pk)
                        [qual-parts qual-revs (qu/getf m qual)]]))])))

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
              (let [id (dm/pk-val m pk)
                    [qual-parts qual-revs rel-maps] (pk->rel-maps id)
                    m (if pk? m (apply dissoc m rempk))]
                (qu/nest-in m qual-parts qual-revs rel-maps)))
            m many-results))))))

(defn multi-query
  "Implementation of :multiple query strategy"
  [query-fn dm q & opts]
  (let [[eq env] (qe/expand-query dm q :expand-joins false)
        ent (-> (env (:from eq)) :resolved :value)
        pk (:pk ent)
        all-fields (filter #(= :field (-> % env :resolved :type))
                           (keys env))
        has-many? (comp #(some (comp not :one :rel) %) :chain env)
        any-many? (boolean (seq (filter has-many? all-fields)))
        [many-fields one-fields] ((juxt filter remove)
                                  has-many?
                                  (:select eq))
        get-path #(or (:final-path (env %)) %)
        many-rel-names (set (map (comp cu/qualifier get-path)
                                 many-fields))
        select-paths (map get-path (:select eq))
        [many-fields one-fields] ((juxt filter remove)
                                   (comp many-rel-names cu/qualifier)
                                   select-paths)
        pk? (dm/pk-present? select-paths pk)
        npk (dm/normalize-pk pk)
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
       "Multi-queries do not support aggregates or group-by on fields from to-many rels"
       {:q q}))
    (let [maps (fetch-one-maps query-fn eq one-fields any-many? opts)
          maps (if-not any-many?
                 (if pk?
                   maps
                   (let [rempk (remove (set select-paths) npk)]
                     (mapv #(apply dissoc % rempk) maps)))
                 (let [ids (mapv #(dm/pk-val % pk) maps)
                       many-groups (group-by cu/qualifier
                                             many-fields)
                       many-results (when (seq maps)
                                      (fetch-many-results
                                        query-fn ent pk npk ids many-groups env opts))]
                   (incorporate-many-results
                     pk pk? npk maps many-results select-paths)))]
    (with-meta
      maps
      {:cantata/query {:from ent :env env :expanded eq}}))))