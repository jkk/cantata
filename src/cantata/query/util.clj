(ns cantata.query.util
  "Tools for query and query result introspection and modification"
  (:require [cantata.data-model :as dm]
            [cantata.util :as cu :refer [throw-info]]
            [cantata.records :as r]
            [clojure.string :as string]
            [honeysql.core :as hq])
  (:import cantata.records.PreparedQuery
           [honeysql.types SqlCall SqlRaw]))

(defn merge-where
  ([q where]
    (merge-where q where :where))
  ([q where clause]
    (if where
      (assoc q clause (if-let [cwhere (clause q)]
                        (if (= :and (first cwhere))
                          (conj cwhere where)
                          [:and cwhere where])
                        where))
      q)))

(def logic-ops #{:and :or :xor :not})

(defn ^:private get-predicate-fields* [x]
  (if (or (map? x) (keyword? x))
    [x]
    (when (vector? x)
      (mapcat get-predicate-fields* x))))

(defn get-predicate-fields
  "Returns all fields contained within a predicate"
  [pred]
  (if (logic-ops (first pred))
    (mapcat get-predicate-fields (rest pred))
    (mapcat get-predicate-fields* (rest pred))))

(defn ^:private replace-predicate-fields* [x smap]
  (if (or (map? x) (keyword? x))
    (or (smap x) x)
    (if (vector? x)
      (mapv #(replace-predicate-fields* % smap) x)
      x)))

(defn replace-predicate-fields
  "Replaces fields in the given predicate according to the given substitution
  map"
  [[op & args] smap]
  (into [op] (if (logic-ops op)
               (map #(replace-predicate-fields % smap) args)
               (map #(replace-predicate-fields* % smap) args))))

(defn ^:private remove-predicate-fields* [[op & args] fields]
  (if (logic-ops op)
    (if-let [args* (seq (remove #{::sentinel}
                                (map #(remove-predicate-fields* % fields)
                                     args)))]
      (into [op] args*)
      ::sentinel)
    (if (some fields (filter keyword? args))
      ::sentinel
      (into [op] args))))

(defn remove-predicate-fields
  "Removes clauses from the given predicate when the clause includes one of
  fields. The fields argument can be a set or function that returns logical
  true when the field should be removed."
  [pred fields]
  (when pred
    (let [pred* (remove-predicate-fields* pred fields)]
      (when (not= ::sentinel pred*)
        pred*))))

(defn expand-predicate-wildcards
  "Expands any wildcards present in pred to :or clauses of fields"
  [pred fields]
  (if-not (sequential? pred)
    pred
    (if (= 1 (count fields))
      (replace-predicate-fields pred {:* (first fields)})
      (let [[op & args] pred]
        (if (logic-ops op)
          (into [op] (map #(expand-predicate-wildcards % fields) args))
          (if (some #{:*} args)
            (vec (cons :or (for [field fields]
                             (into [op] (replace {:* field} args)))))
            (into [op] (map #(expand-predicate-wildcards % fields) args))))))))


(defn get-invalid-predicate-fields
  "Returns any fields in pred that are not valid entity fields or that
  are not contained within allowed-fields. If allowed-fields is not provided,
  all valid entity fields are assumed to be allowed."
  [entity pred & {:keys [allowed-fields]}]
  (let [pred-fields (get-predicate-fields pred)]
    (distinct
      (concat
        (filter #(let [f (dm/resolve entity %)]
                   (or (not f) (not= (:field (:type f)))))
                pred-fields)
        (when (seq allowed-fields)
          (remove (set allowed-fields) pred-fields))))))

(defn valid-predicate?
  "Returns true if pred contains only fields from allowed-fields"
  [entity pred & {:keys [allowed-fields]}]
  (empty? (get-invalid-predicate-fields
            entity pred :allowed-fields allowed-fields)))

(defn wildcard?
  "Returns true if field is a wildcard. E.g., :foo.*"
  [field]
  (and (keyword? field)
       (let [s ^String (name field)]
         (and (.endsWith s ".*")
              (not (.startsWith s "%"))))))

(def aggregate-ops #{:count :min :max :avg :sum :count-distinct
                     :stddev :variance})

(def ^:private aggregate-re
  (re-pattern
    (str "^%(" (string/join "|" (map name aggregate-ops)) ")\\.")))

(defn parse-agg [agg]
  (when (keyword? agg)
    (let [s ^String (name agg)]
      (when (= \% (.charAt s 0))
        (let [doti (.indexOf s (int \.) (int 0))]
          (when-not (neg? doti)
            [(keyword (subs s 1 doti))
             (keyword (subs s (inc doti)))]))))))

(defn agg? [x]
  (and (keyword? x)
       (re-find aggregate-re (name x))))

(defn agg [op path]
  (when-not (aggregate-ops op)
    (throw-info ["Invalid aggregate op:" op] {:op op}))
  (cu/join-path (str "%" (name op)) path))

(defn param? [x]
  (and (keyword? x)
       (= \? (.charAt ^String (name x) 0))))

(defn param-name [x]
  (keyword (subs (name x) 1)))

(defn raw [s]
  (hq/raw s))

(defn call [fn-name & args]
  (apply hq/call fn-name args))

(def join-clauses [:join :left-join :right-join])

(defn get-join-clauses [q]
  (mapcat q join-clauses))

(defn get-join-predicates [q]
  (take-nth 2 (rest (get-join-clauses q))))

(defn get-join-fields [q]
  (mapcat get-predicate-fields (get-join-predicates q)))

(defn get-join-aliases [q]
  (map #(if (vector? %) (second %) %)
       (take-nth 2 (get-join-clauses q))))

(defn get-all-fields
  "Returns all fields in the given query"
  [q]
  (distinct
    (concat (:select q)
            (get-predicate-fields (:where q))
            (get-predicate-fields (:having q))
            (get-join-fields q)
            (:group-by q)
            (map #(if (coll? %) (first %) %) (:order-by q)))))

(defn first-select-field
  "Returns the first selected field in a *non-normalized* query's :select
  clause"
  [q]
  (let [select (if (sequential? q)
                 (second (first (filter #(= :select (first %))
                                        (partition 2 q))))
                 (:select q))]
    (if (sequential? select)
      (first select)
      select)))

(defn call? [x]
  (instance? SqlCall x))

(defn raw? [x]
  (instance? SqlRaw x))

(defn plain-sql?
  "Returns true if q is a plain SQL string or clojure.java.jdbc-style
  [sql params] vector"
  [q]
  (or (string? q)
      (and (vector? q) (string? (first q)))
      (and (sequential? q) (string? (first q)))))

(defn prepared?
  "Returns true if q is a PreparedQuery instance"
  [q]
  (instance? PreparedQuery q))

(defn add-limit-1
  "Adds a \"limit 1\" clause to query q, which can be a Cantata query or plain
  SQL string"
  [q]
  (if (prepared? q)
    q
    (let [q (if (or (map? q) (string? q)) [q] q)
        qargs1 (first q)]
      (if (string? qargs1) ;support plain SQL
        (if (re-find #"(?i)\blimit\s+\d+" qargs1)
          q
          (cons (str qargs1 " LIMIT 1") (rest q)))
        (concat q [:limit 1])))))

;;;;

(defn build-result-map
  "Given field names and values, returns a map that represents a single
  query result. If the :ordered-maps key of opts is true, returns an
  ordered map, otherwise an ordered one."
  ([fnames fvals]
    (build-result-map fnames fvals nil))
  ([fnames fvals opts]
    (if (:ordered-maps opts)
      (cu/zip-ordered-map fnames fvals)
      (zipmap fnames fvals))))

(defn getf
  "See `cantata.core/getf`."
  [qr path]
  (let [[qr path] (if (keyword? path)
                    [qr path]
                    [path qr])]
      (or
        (path qr)
        (let [ks (if (agg? path)
                   [path]
                   (cu/split-path path))]
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

;;;;

(defn get-qual-parts-reverses [qual qual-parts env]
  (loop [qual qual
         revs []]
    (if-not qual
      (into [] (reverse revs))
      (recur
        (first (cu/unqualify qual))
        (conj revs (some (comp not :one :rel) (-> qual env :chain)))))))

(defn get-rp-pk [resolved-path default-pk]
  (if-let [chain (not-empty (:chain resolved-path))]
    (-> chain peek :to :pk)
    default-pk))

(defn nest-in [m [k & ks] [rev? & revs] v]
  (if ks
    (let [nv (nest-in {} ks revs v)] ;FIXME: should this be an ordered map??
      (assoc m k (if rev? [nv] nv)))
    (assoc m k (if (and (not rev?) (sequential? v))
                 (first v)
                 v))))