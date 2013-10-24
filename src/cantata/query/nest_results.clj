(ns cantata.query.nest-results
  "Turn rows and columns into nicely nested structures. Leverages query
  metadata"
  (:require [cantata.data-model :as dm]
            [cantata.util :as cu]
            [cantata.query.util :as qu]))

#_(defn ^:private next-row-group [[row & rows] key-fn]
  (when row
    (let [key (key-fn row)]
      (loop [group [row]
             rows rows]
        (let [row2 (first rows)]
          (if (and row2 (= key (key-fn row2)))
            (recur (conj group row2) (rest rows))
            [group rows]))))))

#_(defn ^:private group-rows [rows key-fn]
  (lazy-seq
    (when-let [[group rows] (next-row-group rows key-fn)]
      (cons group (group-rows rows key-fn)))))

(defn ^:private juxt-idxs [idxs]
  (apply juxt (map (fn [i] #(nth % i)) idxs)))

(defn ^:private get-key-fn [pk-idxs own-idxs]
  (if (seq pk-idxs)
    (if (= 1 (count pk-idxs))
      (let [pk-idx (first pk-idxs)]
        #(nth % pk-idx))
      (juxt-idxs pk-idxs))
    (if (seq own-idxs)
      (juxt-idxs own-idxs)
      identity)))

(defn ^:private takev [n v]
  (subvec v 0 (min n (count v))))

(defn ^:private dropv [n v]
  (subvec v (min n (dec (count v)))))

(defn ^:private pops [v]
  (loop [pops []
         v (pop v)]
    (if (zero? (count v))
      pops
      (recur (conj pops v) (pop v)))))

(defn ^:private nest-group
  [cols rows col->idx col->info all-path-parts opts path-parts pk-cols]
  (let [pp-len (count path-parts)
        cols* (if (empty? path-parts)
                cols
                (filter #(= path-parts (takev pp-len (nth (col->info %) 1)))
                        cols))
        [own-cols rel-cols] ((juxt filter remove)
                              #(= path-parts (nth (col->info %) 1))
                              cols*)
        pk-idxs	(keep col->idx pk-cols)
        key-fn (get-key-fn pk-idxs (map col->idx own-cols))
        own-cols (remove (:added-paths opts) own-cols)
        own-idxs (map col->idx own-cols)
        own-basenames (map #(nth (col->info %) 2) own-cols)]
    ;; Having this makes nesting/distincting reliable, but also makes queries
    ;; more tedious to write
    #_(when (or (empty? pk-idxs) (not-every? identity pk-idxs))
      (throw-info ["Cannot nest: PK cols" pk-cols "not present in query results"]
                  {:pk-cols pk-cols :cols cols :path-parts path-parts}))
    (if (empty? rel-cols)
      (mapv #(qu/build-result-map own-basenames (map % own-idxs) opts)
            (filter #(every? % pk-idxs) ;nil PK = absent outer-joined row
                    (cu/distinct-key key-fn rows)))
      (let [next-infos (cu/distinct-key
                         first
                         (for [rel-col rel-cols
                               :let [info (col->info rel-col)
                                     rel-pp (nth info 1)]
                               :when (and (= path-parts (takev pp-len rel-pp))
                                          ;; Either one rel away or no intermediate
                                          ;; rels selected
                                          (or (= (inc pp-len) (count rel-pp))
                                              (not-any? all-path-parts
                                                        (pops rel-pp))))]
                           info))]
        (into
          []
          (for [group (vals (cu/ordered-group-by key-fn rows))]
            (reduce
              (fn [m [rel-pk-cols rel-pp _ rel-pp-rev]]
                (let [nest-pp (dropv (count path-parts) rel-pp)
                      nest-pp-rev (dropv (count path-parts) rel-pp-rev)
                      nest-pp-rev (if (< 1 (count nest-pp))
                                    (reduce (fn [revs rev?]
                                              (conj revs (or (peek revs)
                                                             rev?)))
                                            [] nest-pp-rev)
                                   nest-pp-rev)]
                  (qu/nest-in m nest-pp nest-pp-rev
                              (nest-group
                                rel-cols group col->idx col->info
                                all-path-parts opts rel-pp rel-pk-cols))))
              (qu/build-result-map own-basenames (map (first group) own-idxs) opts)
              next-infos)))))))

(defn ^:private get-col-info [from-pk env col]
  (let [rp (env col)
        col-agg? (qu/agg? col)
        path-parts (if col-agg?
                     []
                     (pop (cu/split-path col)))
        [qual basename] (if col-agg?
                          [nil col]
                          (cu/unqualify col))
        path-parts-reverse (qu/get-qual-parts-reverses qual path-parts env)
        pk-cols (mapv #(cu/join-path qual %)
                      (dm/normalize-pk (qu/get-rp-pk rp from-pk)))]
    [pk-cols path-parts basename path-parts-reverse]))

(defn nest-results
  "Given query columns, rows, and query meta data, returns a sequence of maps
  nested as according to the relationships of entities involved in the query."
  ([cols rows from-ent env]
    (nest-results cols rows from-ent env nil))
  ([cols rows from-ent env opts]
    (let [col->idx (zipmap cols (range))
          from-pk (:pk from-ent)
          col->info (zipmap cols (map #(get-col-info from-pk env %) cols))
          all-path-parts (set (map #(nth % 1) (vals col->info)))]
      (nest-group cols rows col->idx col->info all-path-parts opts [] [from-pk]))))