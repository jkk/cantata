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

(defrecord Rel [name ename rk dir])

(defn guess-rk [rname]
  (keyword (str (name rname) "-id")))

(defn make-rel [m]
  (map->Rel
    (let [name (:name m)
          ename (:ename m)]
      (cond-> m
              (not ename) (assoc :ename name)
              (not (:rk m)) (assoc :rk (guess-rk name))
              (not (:dir m)) (assoc :dir :out)))))

(defrecord Entity [name pk fields rels db-name db-schema shortcuts])

(defn make-entity [m]
  (map->Entity
    (let [fields (reduce
                   (fn [fields fspec]
                     (assoc fields (:name fspec) (make-field fspec)))
                   (om/ordered-map)
                   (:fields m))
          rels (reduce
                 (fn [rels rspec]
                   (assoc rels (:name rspec) (make-rel rspec)))
                 (om/ordered-map)
                 (:rels m))]
      (cond-> (assoc m
                     :fields fields
                     :rels rels)
              (not (:db-name m)) (assoc :db-name (guess-db-name (:name m)))
              (not (:pk m)) (assoc :pk (:name (first fields)))))))

;; TODO: caching?
(defrecord DataModel [entities graph])

(defn ^:private add-outgoing-edge [graph ename rel]
  (update-in graph [ename (:name rel)] (fnil conj #{}) rel))

(defn ^:private reverse-rel-name
  ([from]
    (keyword (str "_" (name from))))
  ([rel from]
    (keyword (str "_"
                  (name (:name rel)) "."
                  (name from)))))

(defn ^:private add-incoming-edges [graph rel from]
  (let [ename (:ename rel)
        rrname (reverse-rel-name rel from)
        rrel (make-rel {:name rrname
                        :ename from
                        :rk (:rk rel)
                        :dir :in})
        rrname2 from
        rrel2 (assoc rrel :name rrname2)]
    (-> graph
      (update-in [ename rrname] (fnil conj #{}) rrel)
      (update-in [ename rrname2] (fnil conj #{}) rrel2))))

(defn ^:private add-edges [graph entity]
  (let [ename (:name entity)]
    (reduce
      (fn [g rel]
        (-> g
          (add-outgoing-edge ename rel)
          (add-incoming-edges rel ename)))
      graph
      (vals (:rels entity)))))

(defn data-model [& entity-specs]
  ;; TODO: enforce naming uniqueness, check for bad rels
  (let [[ents g] (reduce
                   (fn [[ents g] espec]
                     (let [ent (make-entity espec)]
                       [(assoc ents (:name ent) ent)
                        (add-edges g ent)]))
                   [(om/ordered-map) {}]
                   entity-specs)]
    (->DataModel ents g)))

(defn reflect-data-model [data-source & entity-specs]
  (apply data-model
         (let [especs (or (not-empty entity-specs)
                          (cds/reflect-entities data-source))]
           (for [espec especs]
             (assoc espec
                    :fields (or (:fields espec)
                                (cds/reflect-fields data-source (:name espec)))
                    :pk (or (:pk espec)
                            (:name (first (:fields espec)))
                            (cds/reflect-pk data-source (:name espec))))))))

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

(defn resolve-rel [dm ename rname]
  (let [rels (get-in dm [:graph ename rname])]
    (cond
      (empty? rels) (throw (ex-info (str "No such rel " rname
                                         " for entity " ename)
                                    {:data-model dm
                                     :ename ename
                                     :rname rname}))
      (< 1 (count rels)) (throw (ex-info (str "Ambiguous rel " rname
                                              " for entity " ename)
                                         {:data-model dm
                                          :ename ename
                                          :rname rname}))
      :else (first rels))))

(defrecord Resolved [type value])

;; TODO: shortcuts
(defn resolve
  ([entity xname]
    (if (identical? xname :*)
      (->Resolved :wildcard :*)
      (if-let [field (get-in entity [:fields xname])]
       (->Resolved :field field)
       (when-let [rel (get-in entity [:rels xname])]
         (->Resolved :rel rel)))))
  ([dm ename xname]
    (resolve (get-in dm [:entities ename]) xname)))

(defrecord ChainLink [path entity rel])

(defn resolve-path [dm ename path]
  (loop [chain []
         ename ename
         rnames (split-path path)
         seen-path []]
    (if (not (next rnames))
      {:chain chain
       :thing (resolve (:entity (peek chain)) (first rnames))}
      (let [rname (first rnames)
            rel (resolve-rel dm ename rname)
            ename* (:ename rel)
            seen-path* (conj seen-path rname)
            link (->ChainLink
                   (apply join-path seen-path*)
                   (get-in dm [:entities ename*])
                   rel)]
        (recur (conj chain link)
               ename*
               (rest rnames)
               seen-path*)))))



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
                 :rels [{:name :resident :ename :person :rk :home-id :dir :in}
                        {:name :worker :ename :person :rk :office-id :dir :in}]}
                {:name :car
                 :fields [{:name :id}
                          {:name :make}
                          {:name :model}
                          {:name :year}]}
                {:name :car-ownership
                 :fields [{:name :id}
                          {:name :owner-id}
                          {:name :car-id}]
                 :rels [{:name :owner :ename :person :rk :owner-id}
                        {:name :car}]}))
  
  (def dm
    (data-model 
      :person {:fields [:id :full-name :home-id :office-id]
               :belongs-to [:home {:ename :location}
                            :office {:ename :location}]
               :has-many [:car {:via :car-ownership :rk :owner-id}]}
      :location {:fields [:id :name :address]
                 :has-many [:resident {:ename :person :rk :home-id}
                            :worker {:ename :person :rk :office-id}]}
      :car {:fields [:id :make :model :year]
            :has-many [:owner {:ename :person :via :car-ownership}]}
      :car-ownership {:fields [:id :owner-id :car-id]
                      :belongs-to [:owner {:ename :person}
                                   :car {}]}))
  
  )