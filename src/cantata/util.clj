(ns cantata.util
  (:require [clojure.string :as string]
            [flatland.ordered.map :as om]))

(set! *warn-on-reflection* true)

(def ^:dynamic *verbose* false)

(defn ^:private reverse-rel-name? [^String s]
  (= \_ (.charAt s 0)))

;; TODO: memoize?
(defn split-path [path]
  (if (sequential? path)
    (if (string? (first path))
      (mapv keyword path)
      path)
    (loop [ret []
           parts (seq (string/split (name path) #"\."))]
     (if (nil? parts)
       ret
       (let [x (first parts)]
         (if (reverse-rel-name? x)
           (recur (conj ret (keyword (str x "." (second parts)))) (nnext parts))
           (recur (conj ret (keyword x)) (next parts))))))))

(defn join-path [& parts]
  (when (seq parts)
    (let [parts (remove nil? parts)]
      (if (= 1 (count parts))
        (let [part1 (first parts)]
          (if (keyword? part1)
            part1
            (keyword (name part1))))
        (keyword (string/join "." (map name parts)))))))

(defn last-part [path]
  (let [s ^String (name path)]
    (when (< 0 (.length s))
      (let [doti (.lastIndexOf s ".")]
        (if (neg? doti)
          path
          (keyword (subs s (inc doti))))))))

(defn unqualify [path]
  (let [s ^String (name path)]
    (when (< 0 (.length s))
      (let [doti (.lastIndexOf s ".")]
        (if (neg? doti)
          [nil path]
          (let [sq ^String (subs s 0 doti)
                doti2 (.lastIndexOf sq ".")]
            (if (and (not (neg? doti2)) (= \_ (.charAt s (inc doti2))))
              [(keyword (subs s 0 doti2))
               (keyword (subs s (inc doti2)))]
              (if (and (neg? doti2) (= \_ (.charAt s 0)))
                [nil (keyword s)]
                [(keyword sq)
                 (keyword (subs s (inc doti)))]))))))))

(defn qualifiers [path]
  (loop [quals []
         [qual] (unqualify path)]
    (if-not qual
      quals
      (recur (conj quals qual) (unqualify qual)))))

(defn unqualified? [path]
  (let [s ^String (name path)]
    (neg? (.indexOf s (int \.) 0))))

(defn collify [x]
  (if (coll? x) x [x]))

(defn seqify [x]
  (if (sequential? x) x [x]))

(defn distinct-key
  "Returns a lazy sequence of the elements of coll with (k element)
  duplicates removed"
  [k coll]
  (let [step (fn step [xs seen]
               (lazy-seq
                ((fn [[f :as xs] seen]
                   (when-let [s (seq xs)]
                     (let [key (k f)]
                       (if (contains? seen key) 
                         (recur (rest s) seen)
                         (cons f (step (rest s) (conj seen key)))))))
                 xs seen)))]
    (step coll #{})))

(defn assoc-if [m pred & kvs]
  (reduce
   (fn [m [k v]]
     (if (pred m k v)
       (assoc m k v)
       m))
   m
   (partition 2 kvs)))

(defn assoc-present [m & kvs]
  (apply assoc-if m
         (fn [m k _]
           (contains? m k))
         kvs))

(defn zip-ordered-map
  [keys vals]
  (loop [map (om/ordered-map)
         ks (seq keys)
         vs (seq vals)]
    (if (and ks vs)
      (recur (assoc map (first ks) (first vs))
             (next ks)
             (next vs))
      map)))

(defn throw-info [msg data]
  (throw
    (ex-info (if (sequential? msg)
               (apply str (interpose " " msg))
               msg)
             data)))