(ns cantata.data-model
  "Data model building and examination

  See cantata.core for the main API entry point"
  (:refer-clojure :exclude [resolve])
  (:require [cantata.reflect :as reflect]
            [cantata.records :as r]
            [cantata.protocols :as cp]
            [cantata.parse :as cpa]
            [cantata.util :as cu :refer [throw-info]]
            [flatland.ordered.map :as om]
            [clojure.string :as string])
  (:import [cantata.records Entity Field Rel DataModel]))

(defn ^:private normalize-spec [spec]
  (cond
    (map? spec) spec
    (keyword? spec) {:name spec}))

(defn guess-db-name
  "Given an entity name, tries to guess the table name"
  [ename]
  (string/replace (name ename) "-" "_"))

(defn guess-rel-key
  "Given a rel name, tries to guess the name of the foreign key"
  [rname]
  (keyword (str (name (cu/last-part rname)) "-id")))

(defn make-field
  "Transform a field spec - a keyword or map - into a Field record"
  [m]
  (let [m (normalize-spec m)]
    (when-not (:name m)
      (throw-info "No :name provided for field" {:rel-spec m}))
    (r/map->Field
      (if (:db-name m)
        m
        (assoc m :db-name (guess-db-name (:name m)))))))

(defn make-rel
  "Transform a rel spec - a keyword or map - into a Rel record"
  [m & {:keys [this-pk this-name other-ents]}]
  (let [m (normalize-spec m)]
    (when-not (:name m)
      (throw-info "No :name provided for rel" {:rel-spec m}))
    (r/map->Rel
      (let [name (:name m)
            ename (:ename m)
            reverse? (:reverse m)]
        (cond-> m
                (not ename) (assoc :ename name)
                (and (not (:key m))) (assoc :key (if reverse?
                                                   this-pk
                                                   (guess-rel-key name)))
                (and (not (:other-key m))) (as-> m
                                                 (assoc m :other-key (if reverse?
                                                                       (guess-rel-key this-name)
                                                                       (:pk (get other-ents (:ename m))))))
                (nil? reverse?) (assoc :reverse false)
                (nil? (:one m)) (assoc :one (not reverse?)))))))

(defn make-shortcut
  "Transform a shortcut spec - a map or [path target-path] vector - into
  a Shortcut record"
  [m]
  (let [m (if (sequential? m)
            {:name (first m)
             :path (second m)}
            m)]
    (when-not (and (:name m) (:path m))
      (throw-info "Shortcut must contain :name and :path"
                  {:shortcut-spec m}))
    (r/map->Shortcut m)))

(defn ^:private ordered-map-by-name [maps f]
  (reduce
    #(let [v (f %2)]
       (assoc %1 (:name v) v))
    (om/ordered-map)
    maps))

(defn make-entity
  "Transform an entity spec - a map - into an Entity record"
  [m]
  (when-not (:name m)
    (throw-info "No :name provided for entity"
                {:entity-spec m}))
  (when-not (seq (:fields m))
    (throw-info "No :fields provided for entity"
                {:entity-spec m}))
  (r/map->Entity
    (let [fields (ordered-map-by-name (:fields m) make-field)
          pk (or (:pk m)
                 (when-let [field1 (first fields)]
                   (key field1))
                 (throw-info ["No :pk provided for entity" (:name m)]
                             {:entity-spec m}))
          rels (ordered-map-by-name (:rels m) #(make-rel %
                                                         :this-name (:name m)
                                                         :this-pk pk))
          shortcuts (ordered-map-by-name (:shortcuts m) make-shortcut)]
      (cond-> (assoc m
                     :fields fields
                     :rels rels
                     :shortcuts shortcuts)
              (not (:db-name m)) (assoc :db-name (guess-db-name (:name m)))
              (not (:pk m)) (assoc :pk pk)
              (:validate m) (as-> m
                                  (assoc-in m [:hooks :validate] (:validate m))
                                  (dissoc m :validate))))))

(defn ^:private reverse-rel-name [rel from]
  (keyword (str "_"
                (name (:name rel)) "."
                (name from))))

(defn ^:private add-generic-reverse-rel [oldrel newrel]
  (if-not oldrel
    newrel
    (when (= (map #(get oldrel %) [:name :ename :key :other-key :reverse])
             (map #(get newrel %) [:name :ename :key :other-key :reverse]))
      oldrel)))

(defn ^:private add-reverse-rels [ents ent rel]
  (if (:reverse rel)
    ents
    (let [ename (:ename rel)
          from (:name ent)
          rrname (reverse-rel-name rel from) ;with qualifier - unique
          rrel (make-rel {:name rrname
                          :ename from
                          :key (:other-key rel)
                          :other-key (:key rel)
                          :reverse true})
          rrname2 from ;without qualifier - not necessarily unique
          rrel2 (assoc rrel :name rrname2)]
      (if (ents ename)
        (-> ents
          (assoc-in [ename :rels rrname] rrel)
          ;; clear out the unqualified rel if necessary, to avoid ambiguity
          (as-> ents
                (if (= from ename)
                  ents
                  (update-in ents [ename :rels rrname2] add-generic-reverse-rel rrel2))))
        ents))))

(declare validate-data-model data-model? entities fields rels shortcuts)

(defn ^:private normalize-entity-specs [specs]
  (if (data-model? specs)
    (for [ent (entities specs)]
      (let [m (into {} ent)]
        (cond-> m 
          (:fields m) (assoc :fields (fields ent))
          (:rels m) (assoc :rels (remove :reverse (rels ent)))
          (:shortcuts m) (assoc :shortcuts (shortcuts ent)))))
    (if (map? specs)
      (for [[k v] specs]
        (assoc v :name k))
      specs)))

(defn make-data-model
  "Transform a data model spec into a DataModel record

  See `cantata.core/data-model` for full docs"
  ([entity-specs]
    (make-data-model nil entity-specs))
  ([name entity-specs]
    (let [entity-specs (normalize-entity-specs entity-specs)]
      (when-let [bad-spec (first (remove map? entity-specs))]
        (throw-info ["Invalid entity spec:" bad-spec]
                    {:entity-spec bad-spec}))
      (let [;; Wait to init rels, so we can guess PK/FKs more reliably
            ents (ordered-map-by-name (map #(dissoc % :rels) entity-specs)
                                      make-entity)
            ents (reduce
                   (fn [ents [ent rspec]]
                     (if rspec
                       (let [rel (make-rel rspec
                                           :this-pk (:pk ent)
                                           :this-name (:name ent)
                                           :other-ents ents)]
                         (-> ents
                           (assoc-in [(:name ent) :rels (:name rel)] rel)
                           (add-reverse-rels ent rel)))
                       ents))
                   ents
                   (for [[ent rspecs] (map list
                                           (vals ents)
                                           (map :rels entity-specs))
                         rspec rspecs]
                     [ent rspec]))]
        (validate-data-model
          (r/->DataModel name ents))))))

(defn data-model?
  "Returns true if x is a DataModel record"
  [x]
  (instance? DataModel x))

(defn ^:private merge-entity-specs [es1 es2]
  (let [g1 (group-by :name es1)
        g2 (group-by :name es2)
        names (distinct (concat (keys g2) (keys g1)))]
    (for [name names]
      (merge (first (g1 name)) (first (g2 name))))))

(defn reflect-data-model
  "Examines the given data source to auto-generate entity specs, and merges
  them with `entity-specs` (latter taking precedence)"
  [ds entity-specs & opts]
  (make-data-model
    (let [especs (merge-entity-specs
                   (apply reflect/reflect-entities ds opts)
                   (normalize-entity-specs entity-specs))]
      (for [espec especs]
        (let [db-name (or (:db-name espec)
                          (guess-db-name (:name espec)))]
          (assoc espec
                 :fields (or (:fields espec)
                             (apply reflect/reflect-fields ds db-name opts))
                 :rels (or (:rels espec)
                           (apply reflect/reflect-rels ds db-name opts))
                 :pk (or (:pk espec)
                         (:name (first (:fields espec)))
                         (apply reflect/reflect-pk ds db-name opts))))))))

;;;;

(defn entities
  "Returns all Entity records in the data model"
  [dm]
  (vals (:entities dm)))

(defn entity
  "Returns the Entity record with the given name in the data model"
  [dm ename]
  (get-in dm [:entities ename]))

(defn entity?
  "Returns true if x is an Entity record"
  [x]
  (instance? Entity x))

(defn fields
  "Returns all Field records for the given entity or entity name in the data
  model"
  ([ent]
    (vals (:fields ent)))
  ([dm ename]
    (fields (entity dm ename))))

(defn field-names
  "Returns the names of all fields for the given entity or entity name in the
  data model"
  ([ent]
    (keys (:fields ent)))
  ([dm ename]
    (field-names (entity dm ename))))

(defn field
  "Returns the Field record with the given name in the given entity or entity
  name in the data model"
  ([ent fname]
    (get-in ent [:fields fname]))
  ([dm ename fname]
    (field (entity dm ename) fname)))

(defn field?
  "Returns true if x is a Field record"
  [x]
  (instance? Field x))

(defn rels
  "Returns all Rel records for the given entity or entity name in the data
  model"
  ([ent]
    (remove nil? (vals (:rels ent))))
  ([dm ename]
    (rels (entity dm ename))))

(defn rel
  "Returns the Rel record with the given name in the given entity or entity
  name in the data model"
  ([ent rname]
    (get-in ent [:rels rname]))
  ([dm ename rname]
    (rel (entity dm ename) rname)))

(defn rel?
  "Returns true if x is a Rel record"
  [x]
  (instance? Rel x))

(defn shortcuts
  "Returns all Shortcut records in the data model"
  ([ent]
    (vals (:shortcuts ent)))
  ([dm ename]
    (shortcuts (entity dm ename))))

(defn shortcut
  "Return the Shortcut record with the given name for the given entity or
  entity name in the data model"
  ([ent sname]
    (get-in ent [:shortcuts sname]))
  ([dm ename sname]
    (shortcut (entity dm ename) sname)))

(defn hook
  "Return the hook function with the given name for the given entity in the
  data model"
  ([ent hname]
    (get-in ent [:hooks hname]))
  ([dm ename hname]
    (hook (entity dm ename) hname)))

(defn normalize-pk
  "Wraps pk in a collection if it's not already one"
  [pk]
  (cu/seqify pk))

(defn pk-val
  "Fetch the (singular or sequential) primary key value from map `m`"
  [m pk]
  (if (sequential? pk)
    (mapv m pk)
    (pk m)))

(defn pk-present?
  "Returns true if m-or-fields contains pk. If pk is composite, all fields
  must be present."
  [m-or-fields pk]
  (let [pk (normalize-pk pk)
        num-pk (count pk)]
    (if (map? m-or-fields)
      (every? #(contains? m-or-fields %) pk)
      (= num-pk
         (count
           (take num-pk (filter (set pk) m-or-fields)))))))

(defn untyped?
  "Returns true if none of the fields in the entity have types"
  ([ent]
    (let [fields (fields ent)]
      (or (empty? fields)
          (every? nil? (map :type fields)))))
  ([dm ename]
    (untyped? (entity dm ename))))

;;;;

(defn invoke-hook
  "Invoke hook `hname` on entity, if available; otherwise return nil"
  [ent hname & args]
  (when-let [hf (hook ent hname)]
    (apply hf ent args)))

(defn maybe-invoke-hook
  "If the entity has a hook `hname`, invoke it; otherwise, return `default`"
  [default ent hname & args]
  (if-let [hf (hook ent hname)]
    (apply hf ent args)
    default))

;;;;

(defn resolve
  "Resolve an unqualified keyword path relative to an entity. Returns a Resolved
  record or nil."
  ([ent xname]
    (if-let [f (field ent xname)]
      (r/->Resolved :field f)
      (if-let [r (rel ent xname)]
        (r/->Resolved :rel r)
        (if-let [sc (shortcut ent xname)]
          (r/->Resolved :shortcut sc)
          (when (= (:name ent) xname)
            (r/->Resolved :entity ent))))))
  ([dm ename xname]
    (resolve (entity dm ename) xname)))

(defn ^:private resolve-shortcut [path shortcuts]
  (if (contains? shortcuts path)
    (resolve-shortcut (shortcuts path) shortcuts)
    path))

(defn resolve-path
  "Resolve a qualified or unqualified keyword path relative to an entity. If
  :lax is true, pretends unresolved path tips refer to entity fields. Returns
  a ResolvedPath record or nil."
  [dm ename-or-entity path & {:keys [lax]}]
  (let [root (if (keyword? ename-or-entity)
               (entity dm ename-or-entity)
               ename-or-entity)]
    (loop [chain []
          ent root
          rnames (cu/split-path path)
          seen-path []
          shortcuts {}]
     (let [rname (first rnames)
           resolved (resolve ent rname)]
       (if (and (not (next rnames))
                (not= :shortcut (:type resolved)))
         (let [final-path (let [joined-seen-path (apply cu/join-path
                                                        (conj seen-path rname))]
                            (or (resolve-shortcut joined-seen-path shortcuts)
                                joined-seen-path))]
           (if (= :rel (:type resolved))
             (let [rel (:value resolved)
                   ent* (entity dm (:ename rel))]
               (r/->ResolvedPath
                 final-path
                 root
                 (conj chain (r/->ChainLink
                               ent
                               ent*
                               (apply cu/join-path seen-path)
                               final-path
                               rel))
                 (r/->Resolved :entity ent*)
                 shortcuts))
             (when-let [resolved (or resolved
                                     (when lax ;pretend it's a field
                                       (r/->Resolved :field (make-field {:name rname}))))]
               (r/->ResolvedPath final-path root chain resolved shortcuts))))
         (condp = (:type resolved)
           :shortcut (let [shortcut-path (-> resolved :value :path)]
                       (recur chain
                              ent
                              (concat (cu/split-path shortcut-path)
                                      (rest rnames))
                              seen-path
                              (assoc shortcuts
                                     (apply cu/join-path (conj seen-path shortcut-path))
                                     (apply cu/join-path (conj seen-path
                                                               (-> resolved :value :name))))))
           :rel (let [rel (:value resolved)
                      ename* (:ename rel)
                      ent* (entity dm ename*)
                      seen-path* (conj seen-path rname)
                      [seen-path* joined-seen-path] (let [joined-seen-path (apply cu/join-path seen-path*)]
                                                      (if-let [sc (resolve-shortcut
                                                                    joined-seen-path shortcuts)]
                                                        [(cu/split-path sc) sc]
                                                        [seen-path* joined-seen-path]))
                      link (r/->ChainLink
                             ent
                             ent*
                             (apply cu/join-path seen-path)
                             joined-seen-path
                             rel)]
                  (recur (conj chain link)
                         ent*
                         (rest rnames)
                         seen-path*
                         shortcuts))
           :entity (recur chain ent (rest rnames) seen-path shortcuts) 
           nil))))))

(defn validate-data-model
  "Ensures all relationship references in the given data model are valid.
  Throws an exception if anything is invalid, otherwise returns the data model."
  [dm]
  (doseq [ent (entities dm)]
    (doseq [rel (rels ent)]
      (when rel
        (let [rent (entity dm (:ename rel))]
          (when-not rent
            (throw-info ["Invalid entity name" (:ename rel)
                         "in rel" (:name rel) "in entity" (:name ent)]
                        {:rel rel :entity ent}))
          (when-let [rname (:name rel)]
            (or (empty? (fields ent))
                (not (field ent rname))
                (throw-info ["Rel name" rname "conflicts with field name"
                             "in entity" (:name ent)]
                            {:rel rel :entity ent})))
          (when-let [key (:key rel)]
            (or (resolve ent key)
                (empty? (fields ent))
                (throw-info ["No field matching rel key" key
                             "in rel" (:name rel) "in entity" (:name ent)]
                            {:rel rel :entity ent})))
          (when-let [other-key (:other-key rel)]
            (or (resolve rent other-key)
                (empty? (fields rent))
                (throw-info ["No field matching rel other-key" other-key
                             "in rel" (:name rel) "in entity" (:name ent)]
                            {:rel rel :entity ent})))))))
  dm)

;;;;

(defn dependency-graph
  "Returns a map of entity names to names of entities on which they depend"
  [dm]
  (let [ents (entities dm)]
    (reduce
      (fn [g [ent rel]]
        (let [rel-ent (entity dm (:ename rel))]
          (update-in g [(:name ent)]
                     conj (:name rel-ent))))
      (into {} (zipmap (map :name ents) (repeat #{})))
      (for [ent ents
            rel (rels ent)
            :when (not (:reverse rel))]
        [ent rel]))))

(defn sort-entities
  "Does a topological sort of all entities and returns the their names"
  [dm]
  (let [deps (dependency-graph dm)]
    (loop [deps deps
           s #{}
           ret []]
      (if (empty? deps)
        ret
        (if-let [ename (ffirst
                         (filter
                           (fn [[ename depnames]]
                             (empty? (remove s depnames)))
                           deps))]
          (recur (dissoc deps ename)
                 (conj s ename)
                 (conj ret ename))
          (throw-info
            "Circular dependency detected" {:deps deps :seen s}))))))

(defn dependent-graph
  "Returns a map of entity names to a set of vectors [dependent-name rel]"
  [dm]
  (let [ents (entities dm)]
    (reduce
      (fn [g [ent rel]]
        (let [rel-ent (entity dm (:ename rel))]
          (update-in g [(:name rel-ent)]
                     conj [(:name ent) rel])))
      (into {} (zipmap (map :name ents) (repeat #{})))
      (for [ent ents
            rel (rels ent)
            :when (not (:reverse rel))]
        [ent rel]))))