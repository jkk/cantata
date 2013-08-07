(ns cantata.query
  (:require [cantata.util :as cu]
            [cantata.data-model :as dm]
            [cantata.records :as r]
            [clojure.string :as string]
            [flatland.ordered.map :as om]))

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
       (.endsWith ^String (name field) ".*")))

(defn resolve-path [dm entity qenv path]
  (or
    ;;TODO create Resolved record
    (when (map? path) {})
    (when-let [resolved (get qenv path)]
      (r/->ResolvedPath entity [] resolved nil))
    (when (get-aggregate-op path) {}) ;;TODO: pass along agg info
    (when dm
      (dm/resolve-path dm entity path))))

(defn expand-wildcard
  "Expands a wildcard field into a sequence of all fields for the given
  entity."
  [dm entity field & [qenv]]
  (let [path (-> (name field)
               (string/replace #"\.\*$" "")
               keyword)
        rent (-> (resolve-path dm entity qenv path)
               :resolved :value)]
    (if-not rent
      (throw (ex-info (str "Unrecognized wildcard: " field)
                      {:field field :entity entity}))
      (map #(cu/join-path path %)
           (dm/field-names rent)))))

(defn expand-wildcards [dm entity fields & [qenv]]
  (mapcat (fn [field]
            (cond
             (= :* field) (dm/field-names entity)
             (wildcard? field) (expand-wildcard dm entity field qenv)
             :else [field]))
          fields))

(defn get-all-fields
  "Returns all fields in the given query"
  [q]
  (distinct
    (concat (:select q)
            (get-predicate-fields (:where q))
            (get-predicate-fields (:having q))
            (mapcat get-predicate-fields
                    (take-nth 2 (rest (mapcat q [:join :left-join :right-join]))))
            (:group-by q)
            (map #(if (coll? %) (first %) %) (:order-by q)))))

(defn- get-without-where [dm ent without]
  (let [without (cu/seqify without)]
    (into
      [:and]
      (for [path without]
        (if-let [rp (dm/resolve-path dm ent path)]
          (let [npk (dm/normalize-pk (-> rp :resolved :value :pk))]
            [:= nil (cu/join-path path (first npk))])
          (throw (ex-info
                   (str "Invalid path in :without clause: " path)
                   {:path path})))))))

(defn- expand-without [dm ent q]
  (merge-where (dissoc q :without)
               (get-without-where dm ent (:without q))))

(defn- merge-on [on where]
  (if where
    [:and on where]
    on))

(defn- build-join-on [rel from to from-alias to-alias]
  (let [{:keys [key other-key]} rel]
    [:=
     (cu/join-path from-alias (or key (:pk from)))
     (cu/join-path to-alias (or other-key (:pk to)))]))

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

(defn- expand-implicit-joins [q resolved-fields env]
  (let [chains (keep (comp seq :chain) resolved-fields)
        joins (mapcat #(build-joins (:chain %) (:shortcuts %))
                      (cu/distinct-key
                        (comp :to-path peek :chain)
                        (filter (comp seq :chain) resolved-fields)))
        joins (apply concat (for [[[_ alias :as to] on] (partition 2 joins)
                                  :when (not (env alias))]
                              [to on]))
        join-clause (if (= :inner (:join-type (:options q)))
                      :join :left-join)]
    (if (empty? joins)
      q
      (assoc q join-clause (doall (distinct-joins
                                    (concat (join-clause q)
                                            joins)))))))

(defn- expand-rel-joins [dm ent q clause]
  (reduce
    (fn [q incl]
      (let [[path opts] incl
            rp (dm/resolve-path dm ent path)
            rent (-> rp :resolved :value)
            _ (when-not rent
                (throw (ex-info (str "Unrecognized path " path " for entity "
                                     (:name ent))
                                {:path path :entity ent})))
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

(defn- expand-rel-select [dm ent q clause]
  (let [incls (clause q)
        q (assoc (dissoc q clause)
                 :select (concat (:select q)
                                 (incls->select incls)))]
    (if (= :with clause)
      (let [preds (for [[path] incls]
                    (let [rent (-> (dm/resolve-path dm ent path) :resolved :value)]
                      (if-not rent
                        (throw (ex-info
                                 (str "Unrecognized path " path " for entity "
                                      (:name ent))
                                 {:path path}))
                        (let [npk (dm/normalize-pk (:pk rent))]
                          (if (= 1 (count npk))
                            [:not= nil (cu/join-path path (first npk))]
                            [:and (vec (for [pk npk]
                                         [:not= nil (cu/join-path path pk)]))])))))]
        (merge-where q (vec (cons :and preds))))
      q)))

(defn get-query-env [dm q]
  (into {} (let [joins (mapcat q [:join :left-join :right-join])]
             (for [[to on] (partition 2 joins)]
               (let [to1 (if (vector? to) (first to) to)
                     [ename subq] (if (map? to1)
                                    [(:from to1) to1]
                                    [to1])
                     alias (if (vector? to)
                             (second to)
                             ename)]
                 [alias (assoc (r/->Resolved
                                :joined-entity
                                (dm/entity dm ename))
                               :on on
                               :subquery subq)])))))

(declare prep-query)

(defn- expand-subqueries [dm q subqs env]
  (let [;; subqueries in :select, :where, and :having
        q (if (empty? subqs)
            q
            (let [smap (zipmap subqs (map #(:q (prep-query dm % :env env)) subqs))]
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
                                          [(:q (prep-query dm el1 :env env))
                                           (second el)]
                                          el))))
                                  (clause q))))))
            q [:join :left-join :right-join])]
    q))

(def aggregate-ops #{:count :min :max :avg :sum :count-distinct
                     :stddev :variance})

(def aggregate-field-re
  (re-pattern
    (str "^%(" (string/join "|" (map name aggregate-ops)) ")\\.")))

(defn get-aggregate-op [field]
  (when-let [op-str (second
                      (re-find aggregate-field-re (name field)))]
    (keyword op-str)))

(defn agg [op field]
  (when-not (aggregate-ops op)
    (throw (ex-info (str "Invalid aggregate op: " op) {:op op})))
  (cu/join-path (str "%" (name op)) field))

(defn resolve-paths [dm ent qenv paths]
  (reduce
    (fn [rps path]
      (if-let [rp (resolve-path dm ent qenv path)]
        (let [rps (assoc rps path rp)
              qual (when (keyword? path)
                     (first (cu/unqualify path)))]
          (if qual
            (assoc rps qual (resolve-path dm ent qenv qual))
            rps))
        (throw (ex-info (str "Unrecognized path " path " for entity "
                             (:name ent))
                        {:path path :entity ent}))))
    (om/ordered-map)
    paths))

(defn prep-query
  "Prepares a query for execution by expanding wildcard fields, implicit joins,
  and subqueries. Returns the transformed query and gathered information."
  [dm qargs & {:keys [expand-joins env] :or {expand-joins true}}]
  (when-not dm
    (throw (ex-info "No data-model provided" {:qargs qargs})))
  (let [q (apply build-query (if (map? qargs) [qargs] qargs))
        ent (dm/entity dm (:from q))
        _ (when-not (and ent (dm/entity? ent))
            (throw (ex-info (str "Invalid :from - " (:from q))
                            {:q q})))
        q (if (:select q) q (assoc q :select [:*]))
        q (if (:include q)
            (if (false? expand-joins)
              (expand-rel-select dm ent q :include)
              (expand-rel-joins dm ent q :include))
            q)
        q (if (:with q)
            (if (false? expand-joins)
              (expand-rel-select dm ent q :with)
              (expand-rel-joins dm ent q :with))
            q)
        qenv (merge env (get-query-env dm q))
        q (assoc q :select (vec (expand-wildcards dm ent (:select q) qenv)))
        q (if (:without q) (expand-without q) q)
        fields (get-all-fields q)
        rps (resolve-paths dm ent qenv fields)
        [q qenv fields rps] (if (false? expand-joins)
                              [q qenv fields rps]
                              (let [q (expand-implicit-joins q (vals rps) qenv)
                                    qenv (merge env (get-query-env dm q))
                                    fields (get-all-fields q)
                                    rps (resolve-paths dm ent nil fields)]
                                [q qenv fields rps]))
        subqs (filter map? fields)
        subq-env (assoc qenv (:name ent) (r/->Resolved
                                           :parent-entity
                                           ent))
        q (expand-subqueries dm q subqs subq-env)]
    {:q q
     :resolved-paths rps}))
