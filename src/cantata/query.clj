(ns cantata.query
  "Implementation of query related features

  Most functions besides those related to normalization operate on normalized
  query maps, not on the looser map/vector format accepted by cantata.core
  functions.

  See cantata.core for the main API entry point."
  (:require [cantata.util :as cu :refer [throw-info]]
            [cantata.data-model :as dm]
            [cantata.records :as r]
            [cantata.reflect :as reflect]
            [clojure.string :as string]
            [flatland.ordered.map :as om]))

(set! *warn-on-reflection* true)

(defn ^:private merge-where
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

(defmulti normalize-clause
  "Given a [clause-name clause-value] vector, normalizes the clause value as
  appropriate. E.g., wraps a non-sequential :select into a collection so
  that it can be operated on generically.

  Dispatches on clause-name."
  (fn [[clause-name clause-val]]
    clause-name))

(defmethod normalize-clause :default [[_ clause-val]]
  clause-val)

(defmethod normalize-clause :select [[_ clause-val]]
  (cu/seqify clause-val))

(declare merge-clauses)

(defn ^:private normalize-incl-opts [opts]
  (cond
    (keyword? opts) {:select [opts]}
    (sequential? opts) {:select opts}
    (map? opts) (into {} (map (juxt first normalize-clause)
                              opts))))

(defn ^:private normalize-include [clause-val]
  (if (map? clause-val)
    (into {} (for [[k v] clause-val]
               [k (normalize-incl-opts v)]))
    (if (sequential? clause-val)
      (zipmap clause-val
              (repeat {:select [:*]}))
      {clause-val {:select [:*]}})))

(defmethod normalize-clause :include [[_ clause-val]]
  (normalize-include clause-val))

(defmethod normalize-clause :with [[_ clause-val]]
  (normalize-include clause-val))

(defmethod normalize-clause :without [[_ clause-val]]
  (cu/seqify clause-val))

(defmethod normalize-clause :order-by [[_ clause-val]]
  (when-not (nil? clause-val)
    (cu/seqify clause-val)))

(defmethod normalize-clause :modifiers [[_ clause-val]]
  (when-not (nil? clause-val)
    (cu/seqify clause-val)))

(defmethod normalize-clause :group-by [[_ clause-val]]
  (cu/seqify clause-val))

(defmulti merge-clause
  "Merges a [clause-name clause-val] pair into the given query map. Dispatches
  on clause-name."
  (fn [q [clause-name clause-val]]
    clause-name))

(defmethod merge-clause :default [q [clause clause-val]]
  (assoc q clause clause-val))

(defmethod merge-clause :where [q [_ clause-val]]
  (merge-where q clause-val))

(defmethod merge-clause :having [q [_ clause-val]]
  (merge-where q clause-val :having))

(defmethod merge-clause :include [q [_ clause-val]]
  (update-in q [:include] merge clause-val))

(defmethod merge-clause :with [q [_ clause-val]]
  (update-in q [:with] merge clause-val))

(defmethod merge-clause :without [q [_ clause-val]]
  (update-in q [:without] concat clause-val))

(defmethod merge-clause :join [q [_ clause-val]]
  (update-in q [:join] concat clause-val))

(defmethod merge-clause :left-join [q [_ clause-val]]
  (update-in q [:left-join] concat clause-val))

(defmethod merge-clause :right-join [q [_ clause-val]]
  (update-in q [:right-join] concat clause-val))

(defmethod merge-clause :options [q [_ clause-val]]
  (update-in q [:options] merge clause-val))

(defn merge-clauses
  "Normalized and merges the given clauses into query map `q`."
  [q & clauses]
  (reduce merge-clause q (map (juxt first normalize-clause)
                              (partition 2 clauses))))

(defn merge-query
  "Merges query map `q2` into query map `q1`. Each clause in `q1` will be
  normalized and then marged using `merge-clause`."
  [q1 q2]
  (reduce merge-clause q1 (map (juxt key normalize-clause) q2)))

(declare build-query)

(defn ^:private expand-from [q]
  (if (map? (:from q))
      (let [inner-q (build-query (:from q))]
        (if (:from inner-q)
          (assoc (merge-query inner-q q)
                 :from (:from inner-q))
          inner-q))
      q))

(defn ^:no-doc build-query
  "Builds a query from the given query arguments and returns a query map with
  normalized and merged clauses."
  [& qargs]
  (let [;; one or more query maps
        q (reduce merge-query {} (take-while map? qargs))
        qargs (drop-while map? qargs)
        ;; keyword clauses
        q (if (seq qargs)
            (apply merge-clauses q qargs)
            q)]
    (expand-from q)))

(def ^:private logic-ops #{:and :or :xor :not})

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

(def ^:private aggregate-ops #{:count :min :max :avg :sum :count-distinct
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

(declare resolve-path)

(defn expand-wildcard
  "Expands a wildcard field into a sequence of all fields for the given
  entity."
  [dm ent env field]
  (let [path (first (cu/unqualify field))
        rent (-> (env path) :resolved :value)]
    (if (or (not rent) (empty? (dm/field-names rent)))
      [field]
      (map #(cu/join-path path %)
           (dm/field-names rent)))))

(defn expand-wildcards
  "Expands any wildcards in `fields` into sequences of relevant entity fields."
  [dm ent fields env]
  (mapcat (fn [field]
            (cond
              (identical? :* field) (or (seq (dm/field-names ent))
                                        [(cu/join-path (:name ent) :*)])
              (wildcard? field) (expand-wildcard dm ent env field)
              (= :entity (-> env field :resolved :type)) (expand-wildcard
                                                           dm ent env (cu/join-path field :*))
              :else [field]))
          fields))

(def ^:private join-clauses [:join :left-join :right-join])

(defn get-join-clauses [q]
  (mapcat q join-clauses))

(defn get-join-predicates [q]
  (take-nth 2 (rest (get-join-clauses q))))

(defn get-join-fields [q]
  (mapcat get-predicate-fields (get-join-predicates q)))

(defn get-join-aliases [q]
  (map #(if (vector? %) (first %) %)
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

(defn ^:private get-without-where [without env]
  (let [without (cu/seqify without)]
    (into
      [:and]
      (for [path without]
        (let [rp (env path)
              npk (dm/normalize-pk (-> rp :resolved :value :pk))]
          [:= nil (cu/join-path path (first npk))])))))

(defn ^:private expand-without [q env]
  (merge-where (dissoc q :without)
               (get-without-where (:without q) env)))

(defn ^:private merge-on [on where]
  (if where
    [:and on where]
    on))

(defn ^:private build-join-on [rel from to from-alias to-alias]
  (let [{:keys [key other-key]} rel
        key (or key (if (:reverse rel)
                      (:pk from)
                      (reflect/guess-rel-key (:name rel))))
        other-key (or other-key (if (:reverse rel)
                                  (reflect/guess-rel-key (:name from))
                                  (:pk to)))]
    [:=
     (cu/join-path from-alias key)
     (cu/join-path to-alias other-key)]))

(defn ^:private build-joins [chain shortcuts & [where]]
  (when (seq chain)
    (let [link (first chain)
          {:keys [from to from-path to-path rel]} link
          from-alias (or (get shortcuts from-path)
                         from-path)
          to-alias (or (get shortcuts to-path)
                       to-path)
          pred (build-join-on rel from to from-alias to-alias)
          join [[(:name to) to-alias] (merge-on pred where)]]
      (concat join (build-joins (rest chain) shortcuts)))))

(defn ^:private distinct-joins [joins]
  (apply concat (cu/distinct-key (comp second first)
                                 (partition 2 joins))))

(defn ^:private expand-implicit-joins [q env]
  (let [chains (keep (comp seq :chain) (vals env))
        already-joined (set (get-join-aliases q))
        new-joins (cu/distinct-key
                    (comp :to-path peek :chain)
                    (filter (fn [rp]
                              (let [chain (:chain rp)]
                                (and (seq chain)
                                     (not (already-joined
                                            (-> chain peek :to-path))))))
                            (vals env)))
        joins (mapcat #(build-joins (:chain %) (:shortcuts %))
                      new-joins)
        join-clause (if (= :inner (:join-type (:options q)))
                      :join :left-join)]
    (if (empty? joins)
      q
      (assoc q join-clause (doall (distinct-joins
                                    (concat (join-clause q)
                                            joins)))))))

(defn ^:private expand-rel-joins [q env clause]
  (reduce
    (fn [q incl]
      (let [[path opts] incl
            rp (env path)
            rent (-> rp :resolved :value)
            joins (build-joins (:chain rp) (:shortcuts rp) (:where opts))
            join-clause (if (= :include clause)
                          :left-join :join)
            incl-fields (map #(cu/join-path path %)
                             (:select opts))]
        (assoc q
               :select (concat (:select q) incl-fields)
               join-clause (distinct-joins
                             (concat (join-clause q)
                                     joins)))))
    (dissoc q clause)
    (clause q)))

(defn ^:private incls->select [incls]
  (mapcat
    (fn [[path opts]]
      (map #(cu/join-path path %) (:select opts)))
    incls))

(defn ^:private expand-rel-select [q env clause]
  (let [incls (clause q)
        q (assoc (dissoc q clause)
                 :select (concat (:select q)
                                 (incls->select incls)))]
    (if (= :with clause)
      (let [preds (for [[path] incls]
                    (let [rent (-> (env path) :resolved :value)
                          npk (dm/normalize-pk (:pk rent))]
                      (if (= 1 (count npk))
                        [:not= nil (cu/join-path path (first npk))]
                        [:and (vec (for [pk npk]
                                     [:not= nil (cu/join-path path pk)]))])))]
        (merge-where q (vec (cons :and preds))))
      q)))

(defn ^:private get-query-env [dm ent q & [env]]
  (into (or env {})
        (let [joins (get-join-clauses q)]
          (for [[to on] (partition 2 joins)
                :let [to1 (if (vector? to) (first to) to)
                      [ename subq] (if (map? to1)
                                     [(:from to1) to1]
                                     [to1])
                      alias (if (vector? to)
                              (second to)
                              ename)]
                :when (not (get env alias))]
            (let [jent (dm/entity dm ename)
                  resolved (r/->Resolved :joined-entity jent)]
              [alias (r/->ResolvedPath alias ent [] resolved nil)])))))

(declare expand-query)

(defn ^:private expand-subqueries [dm q subqs env]
  (let [;; subqueries in :select, :where, and :having
        q (if (empty? subqs)
            q
            (let [smap (zipmap subqs (map #(first (expand-query dm % :env env)) subqs))]
              (cu/assoc-present
                q
                :select (vec (replace smap (:select q)))
                :where (replace-predicate-fields (:where q) smap)
                :having (replace-predicate-fields (:having q) smap))))
        ;; subqueries in join clauses
        q (reduce
            (fn [q clause]
              (if-not (contains? q clause)
                q
                (assoc q
                       clause (vec
                                (map-indexed
                                  (fn [i el]
                                    (if (odd? i)
                                      el
                                      (let [el1 (first el)]
                                        (if (and (map? el1)
                                                 (not (dm/entity? el1)))
                                          [(first (expand-query dm el1 :env env))
                                           (second el)]
                                          el))))
                                  (clause q))))))
            q [:join :left-join :right-join])]
    q))

(defn ^:private resolve-joined-field [env path]
  (let [[qual basename] (cu/unqualify path)]
    (when-let [qrp (get env qual)]
      (when (= :joined-entity (-> qrp :resolved :type))
        (let [jent (-> qrp :resolved :value)
              jfield (dm/field jent basename)]
          (when jfield (r/->ResolvedPath
                         path
                         jent []
                         (r/->Resolved :joined-field jfield)
                         nil)))))))

(defn resolve-path
  "Attempts to resolve a path against a data model and a query environment.
  Returns a ResolvedPath record or nil."
  [dm ent env path & opts]
  (or
    (get env path)
    (resolve-joined-field env path)
    (when-let [[agg-op agg-path] (parse-agg path)]
      (when-let [rp (apply resolve-path dm ent env agg-path opts)]
        (let [resolved (r/->Resolved
                         :agg-op (r/->AggOp agg-op agg-path (:resolved rp)))]
          (r/->ResolvedPath path ent (:chain rp) resolved (:shortcuts rp)))))
    (when (param? path)
      (r/->ResolvedPath path ent [] (r/->Resolved :param path) nil))
    (when dm
      (apply dm/resolve-path dm ent path opts))))

(defn ^:private resolve-and-add-path [dm ent env path & opts]
  (if (or (env path) (wildcard? path) (identical? :* path))
    env
    (if-let [rp (apply resolve-path dm ent env path opts)]
      (let [env (assoc env path rp)
            final-path (:final-path rp)]
        (if (= final-path path)
          env
          (assoc env final-path rp)))
      (throw-info ["Unrecognized path" path "for entity" (:name ent)]
                  {:path path :entity ent}))))

(defn resolve-and-add-paths
  "Resolves each path in `paths` and adds the mapping to `env`, which is
  returned. Throws an exception if any path does not resolve."
  [dm ent env paths]
  (let [no-fields? (empty? (dm/fields ent))]
    (reduce
      (fn [env path]
        (let [quals (when (and (keyword? path) (not (agg? path)))
                      (cu/qualifiers path))
              env (reduce
                    (fn [env qual]
                      (resolve-and-add-path dm ent env qual))
                    env quals)]
          (resolve-and-add-path dm ent env path :lax no-fields?)))
      env
      (remove map? paths))))

(defn get-from
  "Returns the explicit or implied target entity name of a query map"
  [q]
  (or (:from q)
      (let [quals (for [path (:select q)]
                    (cu/qualifiers path))]
        (or (ffirst (filter #(= 1 (count %)) quals))
            (peek (first (filter seq quals)))))
      (first (:select q))))

(defn ^:private get-rp-pk [resolved-path default-pk]
  (if-let [chain (not-empty (:chain resolved-path))]
    (-> chain peek :to :pk)
    default-pk))

(defn force-pks
  "If the PK of entity `ent` and any selected rels are not already present in
  query map `eq`, and there are to-many rels referred to, add the PK (or
  multiple PKs) to :select. Returns [q env added-paths]."
  [ent eq env]
  (if (not-any? (comp #(some (comp :reverse :rel) %) :chain)
                (vals env))
    [eq env] ;don't bother if there are no to-many rels
    (let [npk (dm/normalize-pk (:pk ent))
          select (map #(or (:final-path (env %)) %) (:select eq))
          rpks (mapcat
                 (fn [rp]
                   (when-let [rpk (get-rp-pk rp nil)]
                     (let [path (:final-path rp)
                           qual (cu/qualifier path)]
                       (map #(cu/join-path qual %)
                            (dm/normalize-pk rpk)))))
                 (keep env (remove agg? select)))
          all-pks (distinct (concat npk rpks))
          add-pks (remove (set select) all-pks)
          eq (if (seq add-pks)
               (assoc eq :select (concat select add-pks))
               eq)]
      [eq env add-pks])))

(defn ^:private normalize-query-args [qargs]
  (cond
    (sequential? qargs) qargs
    (map? qargs) [qargs]
    (keyword? qargs) [{:from qargs}]
    :else (throw-info "Invalid query format" {:q qargs})))

;; FIXME: need nested environments for subqueries to work
(defn expand-query
  "Prepares a query for execution by expanding wildcard fields, implicit joins,
  and subqueries. Returns the transformed query, a map of resolved paths, and
  any added paths: [q env added-paths]."
  [dm qargs & {:keys [expand-joins env force-pk] :or {expand-joins true}}]
  (let [q (apply build-query (normalize-query-args qargs))
        q (if (:select q) q (assoc q :select [:*]))
        from (or (get-from q)
                 (throw-info "No :from found in query" {:q q}))
        ent (dm/entity dm from)
        _ (when-not (and ent (dm/entity? ent))
            (throw-info ["Unrecognized :from -" from] {:q q}))
        q (if (:from q) q (assoc q :from from))
        env (merge {(:name ent) (dm/resolve-path dm ent (:name ent))}
                   (get-query-env dm ent q env))
        env (resolve-and-add-paths
              dm ent env (concat (keys (:include q))
                                 (keys (:with q))
                                 (:without q)))
        q (if (:include q)
            (if (false? expand-joins)
              (expand-rel-select q env :include)
              (expand-rel-joins q env :include))
            q)
        q (if (:with q)
            (if (false? expand-joins)
              (expand-rel-select q env :with)
              (expand-rel-joins q env :with))
            q)
        q (if (:without q)
            (expand-without q env)
            q)
        env (resolve-and-add-paths dm ent env (:select q))
        q (assoc q :select (vec (expand-wildcards dm ent (:select q) env)))
        env (resolve-and-add-paths dm ent env (get-all-fields q))
        [q env] (if (false? expand-joins)
                  [q env]
                  (let [q (expand-implicit-joins q env)
                        env (get-query-env dm ent q env)
                        env (resolve-and-add-paths
                              dm ent env (get-join-fields q))]
                    [q env]))
        ;; TODO: subqueries
        ;subqs (filter map? fields)
        ;q (expand-subqueries dm q subqs env)
        ]
    (if force-pk
      (force-pks ent q env)
      [q env])))

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

(defn build-key-pred
  "Given a key name or names and one or more ids, builds and returns a
  predicate which will match the keys to ids using :=, :in, etc."
  [pk id-or-ids]
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

(defn nest-in [m [k & ks] [rev? & revs] v]
  (if ks
    (let [nv (nest-in {} ks revs v)]
      (assoc m k (if rev? [nv] nv)))
    (assoc m k (if (and (not rev?) (sequential? v))
                 (first v)
                 v))))

(defn build-result-map
  "Given field names and values, returns a map that represents a single
  query result. If the :unordered-maps key of opts is true, returns an
  unordered map, otherwise an ordered one."
  ([fnames fvals]
    (build-result-map fnames fvals nil))
  ([fnames fvals opts]
    (if (:unordered-maps opts)
      (zipmap fnames fvals)
      (cu/zip-ordered-map fnames fvals))))

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
      (mapv #(build-result-map own-basenames (map % own-idxs) opts)
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
          (for [group (vals (group-by key-fn rows))]
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
                  (nest-in m nest-pp nest-pp-rev
                           (nest-group
                             rel-cols group col->idx col->info
                             all-path-parts opts rel-pp rel-pk-cols))))
              (build-result-map own-basenames (map (first group) own-idxs) opts)
              next-infos)))))))

(defn ^:private get-qual-parts-reverses [qual qual-parts env]
  (loop [qual qual
         revs []]
    (if-not qual
      (into [] (reverse revs))
      (recur
        (first (cu/unqualify qual))
        (conj revs (some (comp :reverse :rel) (-> qual env :chain)))))))

(defn ^:private get-col-info [from-pk env col]
  (let [rp (env col)
        col-agg? (agg? col)
        path-parts (if col-agg?
                     []
                     (pop (cu/split-path col)))
        [qual basename] (if col-agg?
                          [nil col]
                          (cu/unqualify col))
        path-parts-reverse (get-qual-parts-reverses qual path-parts env)
        pk-cols (mapv #(cu/join-path qual %)
                      (dm/normalize-pk (get-rp-pk rp from-pk)))]
    [pk-cols path-parts basename path-parts-reverse]))

(defn nest
  "Given query columns, rows, and query meta data, returns a sequence of maps
  nested as according to the relationships of entities involved in the query."
  ([cols rows from-ent env]
    (nest cols rows from-ent env nil))
  ([cols rows from-ent env opts]
    (let [col->idx (zipmap cols (range))
          from-pk (:pk from-ent)
          col->info (zipmap cols (map #(get-col-info from-pk env %) cols))
          all-path-parts (set (map #(nth % 1) (vals col->info)))]
      (nest-group cols rows col->idx col->info all-path-parts opts [] [from-pk]))))

;;;;

(defn getf
  "See `cantata.core/getf`."
  [qr path]
  (let [[qr path] (if (keyword? path)
                    [qr path]
                    [path qr])]
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
             :where (build-key-pred pk ids)
             :modifiers [:distinct]
             :order-by pk}
          maps (apply query-fn q opts)
          qual-parts (cu/split-path qual)
          qual-revs (get-qual-parts-reverses qual qual-parts env)]
      [qual (into {} (for [m maps]
                       [(dm/pk-val m pk)
                        [qual-parts qual-revs (getf m qual)]]))])))

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
                (nest-in m qual-parts qual-revs rel-maps)))
            m many-results))))))

(defn multi-query
  "Implementation of `cantata.core/querym`. See that function for more info."
  [query-fn dm q & opts]
  (let [[eq env] (expand-query dm q :expand-joins false)
        ent (-> (env (:from eq)) :resolved :value)
        pk (:pk ent)
        all-fields (filter #(= :field (-> % env :resolved :type))
                           (keys env))
        has-many? (comp #(some (comp :reverse :rel) %) :chain env)
        any-many? (boolean (seq (filter has-many? all-fields)))
        [many-fields one-fields] ((juxt filter remove)
                                  has-many?
                                  (:select eq))
        get-path #(or (:final-path env %) %)
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
      {:cantata.core/query {:from ent :env env :expanded eq}}))))