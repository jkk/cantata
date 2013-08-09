(ns cantata.data-model
  (:refer-clojure :exclude [resolve])
  (:require [cantata.reflect :as reflect]
            [cantata.records :as r]
            [cantata.util :as cu :refer [throw-info]]
            [flatland.ordered.map :as om]
            [clojure.string :as string])
  (:import [cantata.records Entity Field Rel DataModel]))

(defn normalize-spec [spec]
  (cond
    (map? spec) spec
    (keyword? spec) {:name spec}))

(defn make-field [m]
  (let [m (normalize-spec m)]
    (when-not (:name m)
      (throw-info "No :name provided for field" {:rel-spec m}))
    (r/map->Field
      (if (:db-name m)
        m
        (assoc m :db-name (reflect/guess-db-name (:name m)))))))

(defn guess-rel-key [rname]
  (keyword (str (name (cu/last-part rname)) "-id")))

(defn make-rel [m & [other-ents]]
  (let [m (normalize-spec m)]
    (when-not (:name m)
      (throw "No :name provided for rel" {:rel-spec m}))
    (r/map->Rel
      (let [name (:name m)
            ename (:ename m)]
        (cond-> m
                (not ename) (assoc :ename name)
                (and (not (:key m))
                     (not (:reverse m))) (assoc :key (guess-rel-key name))
                (and (not (:other-key m))
                     other-ents) (as-> m
                                       (assoc m :other-key (:pk (other-ents (:ename m)))))
                (nil? (:reverse m)) (assoc :reverse false))))))

(defn make-shortcut [m]
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

(defn make-entity [m]
  (when-not (:name m)
    (throw-info "No :name provided for entity"
                {:entity-spec m}))
  (r/map->Entity
    (let [fields (ordered-map-by-name (:fields m) make-field)
          pk (or (:pk m)
                 (when-let [field1 (first fields)]
                   (key field1))
                 (throw-info ["No :pk provided for entity" (:name m)]
                             {:entity-spec m}))
          rels (ordered-map-by-name (:rels m) make-rel)
          shortcuts (ordered-map-by-name (:shortcuts m) make-shortcut)]
      (cond-> (assoc m
                     :fields fields
                     :rels rels
                     :shortcuts shortcuts)
              (not (:db-name m)) (assoc :db-name (reflect/guess-db-name (:name m)))
              (not (:pk m)) (assoc :pk pk)))))

(defn ^:private reverse-rel-name [rel from]
  (keyword (str "_"
                (name (:name rel)) "."
                (name from))))

(defn ^:private add-reverse-rels [ents ent rel]
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
        (update-in [ename :rels rrname2] #(when-not %1 %2) rrel2))
      ents)))

(declare validate-data-model data-model? entities fields rels shortcuts)

(defn normalize-entity-specs [specs]
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

(defn data-model [entity-specs]
  ;; TODO: enforce naming uniqueness
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
                     (let [rel (make-rel rspec ents)]
                       (-> ents
                         (assoc-in [(:name ent) :rels (:name rel)] rel)
                         (add-reverse-rels ent rel)))
                     ents))
                 ents
                 (for [[ent rspecs] (map list (vals ents)
                                         (map :rels entity-specs))
                       rspec rspecs]
                   [ent rspec]))]
      (validate-data-model
        (r/->DataModel ents)))))

(defn data-model? [x]
  (instance? DataModel x))

(defn- merge-entity-specs [es1 es2]
  (let [g1 (group-by :name es1)
        g2 (group-by :name es2)
        names (distinct (concat (keys g2) (keys g1)))]
    (for [name names]
      (merge (first (g1 name)) (first (g2 name))))))

(defn reflect-data-model [ds entity-specs]
  (data-model
    (let [especs (merge-entity-specs
                   (reflect/reflect-entities ds)
                   (normalize-entity-specs entity-specs))]
      (for [espec especs]
        (let [db-name (or (:db-name espec)
                          (reflect/guess-db-name (:name espec)))]
          (assoc espec
                 :fields (or (:fields espec)
                             (reflect/reflect-fields ds db-name))
                 :rels (or (:rels espec)
                           (reflect/reflect-rels ds db-name))
                 :pk (or (:pk espec)
                         (:name (first (:fields espec)))
                         (reflect/reflect-pk ds db-name))))))))

;;;;

(defn entities [dm]
  (vals (:entities dm)))

(defn entity [dm ename]
  (get-in dm [:entities ename]))

(defn entity? [x]
  (instance? Entity x))

(defn fields
  ([ent]
    (vals (:fields ent)))
  ([dm ename]
    (fields (entity dm ename))))

(defn field-names
  ([ent]
    (keys (:fields ent)))
  ([dm ename]
    (field-names (entity dm ename))))

(defn field
  ([ent fname]
    (get-in ent [:fields fname]))
  ([dm ename fname]
    (field (entity dm ename) fname)))

(defn field? [x]
  (instance? Field x))

(defn rels
  ([ent]
    (vals (:rels ent)))
  ([dm ename]
    (rels (entity dm ename))))

(defn rel
  ([ent rname]
    (get-in ent [:rels rname]))
  ([dm ename rname]
    (rel (entity dm ename) rname)))

(defn rel? [x]
  (instance? Rel x))

(defn shortcuts
  ([ent]
    (vals (:shortcuts ent)))
  ([dm ename]
    (shortcuts (entity dm ename))))

(defn shortcut
  ([ent sname]
    (get-in ent [:shortcuts sname]))
  ([dm ename sname]
    (shortcut (entity dm ename) sname)))

(defn normalize-pk [pk]
  (cu/seqify pk))

;;;;

(defn resolve
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

(defn resolve-path [dm ename-or-entity path & {:keys [lax]}]
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
                            (or (shortcuts joined-seen-path)
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
                                                      (if-let [sc (shortcuts joined-seen-path)]
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
          (when-let [key (:key rel)]
            (or (resolve ent key)
                (empty? (fields ent))
                (throw-info ["Invalid rel :key" key
                             "in rel" (:name rel) "in entity" (:name ent)]
                            {:rel rel :entity ent})))
          (when-let [other-key (:other-key rel)]
            (or (resolve rent other-key)
                (empty? (fields rent))
                (throw-info ["Invalid rel :other-key" key
                             "in rel" (:name rel) "in entity" (:name ent)]
                            {:rel rel :entity ent})))))))
  dm)

