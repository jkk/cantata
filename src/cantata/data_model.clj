(ns cantata.data-model
  (:refer-clojure :exclude [resolve])
  (:require [cantata.data-source :as cds]
            [flatland.ordered.map :as om]
            [clojure.string :as string]))

(defn guess-db-name [ename]
  (string/replace (name ename) "-" "_"))

(defrecord Field [name type db-name db-type])

(defn make-field [m]
  (map->Field
    (if (:db-name m)
      m
      (assoc m :db-name (guess-db-name (:name m))))))

(defrecord Rel [name ename key local-key reverse])

(defn guess-rel-key [rname]
  (keyword (str (name rname) "-id")))

(defn make-rel [m]
  (map->Rel
    (let [name (:name m)
          ename (:ename m)]
      (cond-> m
              (not ename) (assoc :ename name)
              (not (:key m)) (assoc :key (guess-rel-key name))
              (nil? (:reverse m)) (assoc :reverse false)))))

(defrecord Shortcut [name path])

(defn make-shortcut [m]
  (map->Shortcut m))

(defrecord Entity [name pk fields rels db-name db-schema shortcuts])

(defn ^:private ordered-map-by-name [maps f]
  (reduce
    #(assoc %1 (:name %2) (f %2))
    (om/ordered-map)
    maps))

(defn make-entity [m]
  (map->Entity
    (let [fields (ordered-map-by-name (:fields m) make-field)
          pk (or (:pk m)
                 (key (first fields)))
          rels (ordered-map-by-name
                 (map #(assoc % :local-key pk) (:rels m))
                 make-rel)
          shortcuts (ordered-map-by-name (:shortcuts m) make-shortcut)]
      (cond-> (assoc m
                     :fields fields
                     :rels rels
                     :shortcuts shortcuts)
              (not (:db-name m)) (assoc :db-name (guess-db-name (:name m)))
              (not (:pk m)) (assoc :pk pk)))))

;; TODO: caching?
(defrecord DataModel [entities])

(defn ^:private reverse-rel-name [rel from]
  (keyword (str "_"
                (name (:name rel)) "."
                (name from))))

(defn ^:private add-reverse-rels [ents ent rel]
  (let [ename (:ename rel)
        from (:name ent)
        rrname (reverse-rel-name rel from)
        rrel (make-rel {:name rrname
                        :ename from
                        :key (:key rel)
                        :local-key (:local-key rel)
                        :reverse true})
        rrname2 from
        rrel2 (assoc rrel :name rrname2)]
    (-> ents
      (assoc-in [ename :rels rrname] rrel)
      (update-in [ename :rels rrname2] #(when-not %1 %2) rrel2))))

(defn data-model [& entity-specs]
  ;; TODO: enforce naming uniqueness, check for bad rels
  (let [ents (ordered-map-by-name entity-specs make-entity)
        ents (reduce
               (fn [ents [ent rel]]
                 (add-reverse-rels ents ent rel))
               ents
               (for [ent (vals ents)
                     rel (vals (:rels ent))]
                 [ent rel]))]
    (->DataModel ents)))

(defn reflect-data-model [data-source & entity-specs]
  (apply
    data-model
    (let [especs (or (not-empty entity-specs)
                     (cds/reflect-entities data-source))]
      (for [espec especs]
        (let [db-name (:db-name espec)]
          (assoc espec
                 :fields (or (:fields espec)
                             (cds/reflect-fields data-source db-name))
                 :rels (or (:rels espec)
                           (cds/reflect-rels data-source db-name))
                 :pk (or (:pk espec)
                         (:name (first (:fields espec)))
                         (cds/reflect-pk data-source db-name))))))))

;;;;

(defn ^:private reverse-rel-name? [^String s]
  (= \_ (.charAt s 0)))

(defn split-path [path]
  (loop [ret []
         parts (seq (string/split (name path) #"\."))]
    (if (nil? parts)
      (map keyword ret)
      (let [x (first parts)]
        (if (reverse-rel-name? x)
          (recur (conj ret (str x "." (second parts))) (nnext parts))
          (recur (conj ret x) (next parts)))))))

(defn join-path [& parts]
  (keyword (string/join "." (map name parts))))

(defrecord Resolved [type value])

(defn resolve
  ([entity xname]
    (if (identical? xname :*)
      (->Resolved :wildcard :*)
      (if-let [field (get-in entity [:fields xname])]
       (->Resolved :field field)
       (if-let [rel (get-in entity [:rels xname])]
         (->Resolved :rel rel)
         (when-let [shortcut (get-in entity [:shortcuts xname])]
           (->Resolved :shortcut shortcut))))))
  ([dm ename xname]
    (resolve (get-in dm [:entities ename]) xname)))

(defrecord ChainLink [path entity rel])

(defrecord ResolvedPath [chain resolved shortcuts])

(defn resolve-path [dm ename path]
  (loop [chain []
         ename ename
         rnames (split-path path)
         seen-path []
         shortcuts {}]
    (let [entity (get-in dm [:entities ename])
          rname (first rnames)
          resolved (or (resolve entity rname)
                       (throw (ex-info (str "Unknown reference " rname
                                            " for entity " ename)
                                       {:data-model dm
                                        :rname rname
                                        :ename ename})))]
      (if (and (not (next rnames))
               (not= :shortcut (:type resolved)))
        (->ResolvedPath chain resolved shortcuts)
        (condp = (:type resolved)
          :shortcut (let [shortcut-path (-> resolved :value :path)]
                      (recur chain
                             ename
                             (concat (split-path shortcut-path)
                                     (rest rnames))
                             seen-path
                             (assoc shortcuts
                                    (apply join-path (conj seen-path shortcut-path))
                                    (apply join-path (conj seen-path
                                                           (-> resolved :value :name))))))
          :rel (if (= :rel (:type resolved))
                 (let [rel (:value resolved)
                       ename* (:ename rel)
                       seen-path* (conj seen-path rname)
                       link (->ChainLink
                              (apply join-path seen-path*)
                              (get-in dm [:entities ename*])
                              rel)]
                   (recur (conj chain link)
                          ename*
                          (rest rnames)
                          seen-path*
                          shortcuts)))
          (throw (ex-info (str "Illegal path part " rname)
                          {:data-model dm
                           :rname rname
                           :resolved resolved
                           :path path})))))))



(comment
  
  (def dm
    (data-model {:name :person
                 :fields [{:name :id}
                          {:name :full-name}
                          {:name :home-id}
                          {:name :office-id}]
                 :rels [{:name :home :ename :location}
                        {:name :office :ename :location}]}
                {:name :location
                 :fields [{:name :id}
                          {:name :name}
                          {:name :address}]
                 :shortcuts [{:name :worker :path :_office.person}
                             {:name :resident :path :_home.person}]}
                {:name :car
                 :fields [{:name :id}
                          {:name :make}
                          {:name :model}
                          {:name :year}]}
                {:name :car-ownership
                 :fields [{:name :id}
                          {:name :owner-id}
                          {:name :car-id}]
                 :rels [{:name :owner :ename :person :key :owner-id}
                        {:name :car}]}))
  
  (def dm
    (data-model 
      :person {:fields [:id :full-name :home-id :office-id]
               :belongs-to [:home {:ename :location}
                            :office {:ename :location}]
               :has-many [:car {:via :car-ownership :key :owner-id}]}
      :location {:fields [:id :name :address]
                 :has-many [:resident {:ename :person :key :home-id}
                            :worker {:ename :person :key :office-id}]}
      :car {:fields [:id :make :model :year]
            :has-many [:owner {:ename :person :via :car-ownership}]}
      :car-ownership {:fields [:id :owner-id :car-id]
                      :belongs-to [:owner {:ename :person}
                                   :car {}]}))
  
  )