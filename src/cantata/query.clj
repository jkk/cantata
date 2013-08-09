(ns cantata.query
  (:require [cantata.util :as cu :refer [throw-info]]
            [cantata.data-model :as dm]
            [cantata.records :as r]
            [clojure.string :as string]
            [flatland.ordered.map :as om]))

(set! *warn-on-reflection* true)

(defn- merge-where
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

(defmulti normalize-clause (fn [[clause clause-val]]
                             clause))

(defmethod normalize-clause :default [[_ clause-val]]
  clause-val)

(defmethod normalize-clause :select [[_ clause-val]]
  (cu/seqify clause-val))

(declare merge-clauses)

(defn- normalize-incl-opts [opts]
  (cond
    (keyword? opts) {:select [opts]}
    (sequential? opts) {:select opts}
    (map? opts) (into {} (map (juxt first normalize-clause)
                              opts))))

(defn- normalize-include [clause-val]
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

(defmulti merge-clause (fn [q [clause clause-val]]
                         clause))

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

(defn merge-clauses [q & clauses]
  (reduce merge-clause q (map (juxt first normalize-clause)
                              (partition 2 clauses))))

(defn merge-query [q1 q2]
  (reduce merge-clause q1 (map (juxt key normalize-clause) q2)))

(declare build-query)

(defn- expand-from [q]
  (if (map? (:from q))
      (let [inner-q (build-query (:from q))]
        (if (:from inner-q)
          (assoc (merge-query inner-q q)
                 :from (:from inner-q))
          inner-q))
      q))

(defn ^:no-doc build-query
  "Builds a query from the given query arguments and returns a query map with
  normalized clauses."
  [& qargs]
  (let [;; one or more query maps
        q (reduce merge-query {} (take-while map? qargs))
        qargs (drop-while map? qargs)
        ;; keyword clauses
        q (if (seq qargs)
            (apply merge-clauses q qargs)
            q)]
    (expand-from q)))

(def logic-ops #{:and :or :xor :not})

(defn- get-predicate-fields* [x]
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

(defn- replace-predicate-fields* [x smap]
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

(defn- remove-predicate-fields* [[op & args] fields]
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

(def aggregate-re
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

(defn expand-wildcard
  "Expands a wildcard field into a sequence of all fields for the given
  entity."
  [dm env field]
  (let [path (-> (name field)
               (string/replace #"\.\*$" "")
               keyword)
        rent (-> (env path) :resolved :value)]
    (if (or (not rent) (empty? (dm/field-names rent)))
      [field]
      (map #(cu/join-path path %)
           (dm/field-names rent)))))

(defn expand-wildcards [dm entity fields env]
  (mapcat (fn [field]
            (cond
             (= :* field) (or (seq (dm/field-names entity))
                              [(keyword (str (name (:name entity)) ".*"))])
             (wildcard? field) (expand-wildcard dm env field)
             :else [field]))
          fields))

(def join-clauses [:join :left-join :right-join])

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

(defn- get-without-where [without env]
  (let [without (cu/seqify without)]
    (into
      [:and]
      (for [path without]
        (let [rp (env path)
              npk (dm/normalize-pk (-> rp :resolved :value :pk))]
          [:= nil (cu/join-path path (first npk))])))))

(defn- expand-without [q env]
  (merge-where (dissoc q :without)
               (get-without-where (:without q) env)))

(defn- merge-on [on where]
  (if where
    [:and on where]
    on))

(defn- build-join-on [rel from to from-alias to-alias]
  (let [{:keys [key other-key]} rel
        key (or key (if (:reverse rel)
                      (:pk from)
                      (dm/guess-rel-key (:name rel))))
        other-key (or other-key (if (:reverse rel)
                                  (dm/guess-rel-key (:name from))
                                  (:pk to)))]
    [:=
     (cu/join-path from-alias key)
     (cu/join-path to-alias other-key)]))

(defn- build-joins [chain shortcuts & [where]]
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

(defn- distinct-joins [joins]
  (apply concat (cu/distinct-key (comp second first)
                                 (partition 2 joins))))

(defn- expand-implicit-joins [q env]
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

(defn- expand-rel-joins [q env clause]
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

(defn- incls->select [incls]
  (mapcat
    (fn [[path opts]]
      (map #(cu/join-path path %) (:select opts)))
    incls))

(defn- expand-rel-select [q env clause]
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

(defn get-query-env [dm ent q & [env]]
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

(declare prep-query)

(defn- expand-subqueries [dm q subqs env]
  (let [;; subqueries in :select, :where, and :having
        q (if (empty? subqs)
            q
            (let [smap (zipmap subqs (map #(first (prep-query dm % :env env)) subqs))]
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
                                          [(first (prep-query dm el1 :env env))
                                           (second el)]
                                          el))))
                                  (clause q))))))
            q [:join :left-join :right-join])]
    q))

(defn- resolve-joined-field [env path]
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

(defn resolve-path [dm ent env path & opts]
  (or
    (get env path)
    (resolve-joined-field env path)
    (when-let [[agg-op agg-path] (parse-agg path)]
      (when-let [rp (apply resolve-path dm ent env agg-path opts)]
        (let [resolved (r/->Resolved
                         :agg-op (r/->AggOp agg-op agg-path (:resolved rp)))]
          (r/->ResolvedPath path ent (:chain rp) resolved (:shortcuts rp)))))
    (when dm
      (apply dm/resolve-path dm ent path opts))))

(defn- resolve-and-add-path [dm ent env path & opts]
  (if (env path)
    env
    (if-let [rp (apply resolve-path dm ent env path opts)]
      (let [env (assoc env path rp)
            final-path (:final-path rp)]
        (if (= final-path path)
          env
          (assoc env final-path rp)))
      (throw-info ["Unrecognized path " path "for entity" (:name ent)]
                  {:path path :entity ent}))))

(defn resolve-and-add-paths [dm ent env paths]
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

(defn get-from [q]
  (or (:from q)
      (first
        (for [path (:select q)
              :let [quals (cu/qualifiers path)]
              :when (= 1 (count quals))]
          (first quals)))))

;; FIXME: need nested environments for subqueries to work
(defn prep-query
  "Prepares a query for execution by expanding wildcard fields, implicit joins,
  and subqueries. Returns the transformed query and a map of resolved paths."
  [dm qargs & {:keys [expand-joins env] :or {expand-joins true}}]
  (let [q (apply build-query (if (map? qargs) [qargs] qargs))
        q (if (:select q) q (assoc q :select [:*]))
        from (or (get-from q)
                 (throw-info "No :from found in query" {:q q}))
        ent (dm/entity dm from)
        _ (when-not (and ent (dm/entity? ent))
            (throw-info ["Invalid :from -" from] {:q q}))
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
    [q env]))

(defn ^:private next-row-group [[row & rows] key-fn]
  (when row
    (let [key (key-fn row)]
      (loop [group [row]
             rows rows]
        (let [row2 (first rows)]
          (if (and row2 (= key (key-fn row2)))
            (recur (conj group row2) (rest rows))
            [group rows]))))))

(defn ^:private group-rows [rows key-fn]
  (lazy-seq
    (when-let [[group rows] (next-row-group rows key-fn)]
      (cons group (group-rows rows key-fn)))))

(defn ^:private get-group-base [group own-cols own-paths]
  (cu/zip-ordered-map
    own-paths (map (first group) own-cols)))

(defn ^:private nest-group-rels [group base rel-paths-cols]
  (reduce
    (fn [m row]
      (reduce
        (fn [m [qual pcs]]
          (update-in m [qual] (fnil conj #{})
                     (reduce (fn [om [p c]]
                               (assoc om p (nth row c)))
                             (om/ordered-map) pcs)))
        m rel-paths-cols))
    base group))

(defn ^:private juxt-cols [cols]
  (apply juxt (map (fn [col] #(nth % col)) cols)))

(defn ^:private get-key-fn [pk colmap own-cols]
  (if pk
    (if (sequential? pk)
      (juxt-cols (map colmap pk))
      (let [pkcol (colmap pk)]
        #(nth % pkcol)))
    (juxt-cols own-cols)))

(defn nest [cols rows pk]
  (let [colmap (zipmap cols (range))
        [own-paths rel-paths] ((juxt filter remove) cu/unqualified? cols)
        own-cols (map colmap own-paths)]
    (if (empty? rel-paths)
      (mapv #(cu/zip-ordered-map cols %) rows) ;no rels, no nesting
      (if (and pk (not-every? colmap (dm/normalize-pk pk)))
        (throw-info ["Cannot nest: PK" pk "not present in query results"]
                    {:pk pk :cols cols})
        (let [rel-paths-cols (reduce
                               (fn [om rel-path]
                                 (let [[qual basename] (cu/unqualify rel-path)]
                                   (update-in om [qual] (fnil conj [])
                                              [basename (colmap rel-path)])))
                               (om/ordered-map)
                               rel-paths)
              key-fn (get-key-fn pk colmap own-cols)]
          (for [group (group-rows rows key-fn)]
            (let [base (get-group-base group own-cols own-paths)]
              (nest-group-rels group base rel-paths-cols))))))))
