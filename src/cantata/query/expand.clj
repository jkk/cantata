(ns cantata.query.expand
  "Expand short-form queries into long-form - e.g., joins and such - and
  resolve paths to their referents"
  (:require [cantata.data-model :as dm]
            [cantata.util :as cu :refer [throw-info]]
            [cantata.query.util :as qu]
            [cantata.query.build :as qb]
            [cantata.records :as r]
            [flatland.ordered.map :as om]))

(declare resolve-path)

(defn new-env []
  (om/ordered-map))

(defn env-get
  "Looks up path in (possibly stacked) env"
  ([env path depth]
    (cond
      (map? env) (env path)
      (vector? env) (get (nth env (- (dec (count env)) depth)) path)))
  ([env path]
    (cond
      (map? env) (env path)
      (vector? env) (first (keep #(get % path) (rseq env))))))

(defn env-assoc
  "Adds a path to (possibly stacked) env"
  [env path v]
  (if (vector? env)
    (assoc env (dec (count env)) (assoc (peek env) path v))
    (assoc env path v)))

(defn env-merge
  "Merges env2 into topmost env stack"
  [env env2]
  (if (vector? env)
    (assoc env (dec (count env)) (merge (peek env) env2))
    (merge env env2)))

(defn immediate-env
  "Returns the most immediate env, or env if not stacked"
  [env]
  (if (vector? env) (peek env) env))

(defn push-env
  "Adds an additional \"stack frame\" to env"
  [env new-env]
  (if (vector? env)
    (conj env new-env)
    [env new-env]))

(defn expand-wildcard
  "Expands a wildcard field into a sequence of all fields for the given
  entity."
  [dm ent env field]
  (let [path (first (cu/unqualify field))
        rent (-> (env-get env path) :resolved :value)]
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
              (qu/wildcard? field) (expand-wildcard dm ent env field)
              (= :entity (-> (env-get env field)
                           :resolved :type)) (expand-wildcard
                                               dm ent env (cu/join-path field :*))
              :else [field]))
          fields))

(defn ^:private get-without-where [without env]
  (let [without (cu/seqify without)]
    (into
      [:and]
      (for [path without]
        (let [rp (env-get env path)
              npk (dm/normalize-pk (-> rp :resolved :value :pk))]
          [:= nil (cu/join-path path (first npk))])))))

(defn ^:private expand-without [q env]
  (qu/merge-where (dissoc q :without)
                  (get-without-where (:without q) env)))

(defn ^:private merge-on [on where]
  (if where
    [:and on where]
    on))

(defn ^:private build-join-on [rel from to from-alias to-alias]
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

(defn ^:private distinct-joins [joins & [already-joined]]
  (let [joins (cond->> (partition 2 joins)
                       already-joined (remove (comp already-joined second first)))]
    (apply concat (cu/distinct-key (comp second first) joins))))

(defn ^:private expand-implicit-joins [q env]
  (let [chains (keep (comp seq :chain)
                     (vals (immediate-env env)))
        already-joined (set (qu/get-join-aliases q))
        new-joins (cu/distinct-key
                    (comp :to-path peek :chain)
                    (filter (fn [rp]
                              (let [chain (:chain rp)]
                                (and (seq chain)
                                     (not (already-joined
                                            (-> chain peek :to-path))))))
                            (vals (immediate-env env))))
        joins (mapcat #(build-joins (:chain %) (:shortcuts %))
                      new-joins)
        join-clause (if (= :inner (:join-type (:options q)))
                      :join :left-join)]
    (if (empty? joins)
      q
      (assoc q join-clause (doall (distinct-joins
                                    (concat (join-clause q)
                                            joins)
                                    already-joined))))))

(defn ^:private expand-rel-joins [q env clause]
  (reduce
    (fn [q incl]
      (let [[path opts] incl
            rp (env-get env path 0)
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
                    (let [rent (-> (env-get env path 0) :resolved :value)
                          npk (dm/normalize-pk (:pk rent))]
                      (if (= 1 (count npk))
                        [:not= nil (cu/join-path path (first npk))]
                        [:and (vec (for [pk npk]
                                     [:not= nil (cu/join-path path pk)]))])))]
        (qu/merge-where q (vec (cons :and preds))))
      q)))

(defn ^:private get-query-env [dm ent q & [env]]
  (let [env (or (immediate-env env) (new-env))]
    (into env
          (let [joins (qu/get-join-clauses q)]
            (for [[to on] (partition 2 joins)
                  :let [[ename alias] (if (vector? to)
                                        to
                                        [to to])
                        ename (if (map? ename)
                                (:from ename)
                                ename)]
                  :when (not (env-get env alias))]
              (let [jent (dm/entity dm ename)
                    resolved (r/->Resolved :joined-entity jent)]
                [alias (r/->ResolvedPath alias ent [] resolved nil)]))))))

(declare expand-query)

(defn ^:private expand-subquery [dm q env]
  (let [[q env] (expand-query dm q :env (push-env env (new-env)))]
    (with-meta q {:cantata/env env})))

(defn ^:private expand-join-subquery [dm env [q env] clause]
  (if-not (contains? q clause)
    [q env]
    (let [q (assoc q
                   clause (vec
                            (map-indexed
                              (fn [i el]
                                (if (odd? i)
                                  el
                                  (let [[t alias] (if (vector? el)
                                                    el
                                                    [el el])]
                                    (if (and (map? t)
                                             (not (dm/entity? t)))
                                      [(expand-subquery dm t env)
                                       alias]
                                      el))))
                              (clause q))))
          joins (qu/get-join-clauses q)
          alias-jenv (for [[to on] (partition 2 joins)
                           :let [[t alias] (if (vector? to)
                                             to
                                             [to to])]
                           :when (map? t)]
                       [alias (:cantata/env (meta t))])
          env (reduce
                (fn [env [jpath jrp]]
                  (env-assoc env jpath jrp))
                env
                (for [[alias jenv] alias-jenv
                      [jpath jrp] (immediate-env jenv)
                      :when (keyword? jpath)
                      :let [final-path (cu/join-path alias jpath)
                            jrp (assoc jrp
                                       :final-path final-path
                                       :resolved (r/->Resolved
                                                   :joined-field
                                                   (:value (:resolved jrp))))]]
                  [final-path jrp]))]
      [q env])))

(defn ^:private expand-subqueries [dm q subqs env]
  (let [;; subqueries in join clauses
        [q env] (reduce
                  #(expand-join-subquery dm env %1 %2)
                  [q env] qu/join-clauses)
        ;; subqueries in :select, :where, and :having
        q (if (empty? subqs)
            q
            (let [smap (zipmap subqs
                               (map #(expand-subquery dm % env)
                                    subqs))]
              (cu/assoc-present
                q
                :select (vec (replace smap (:select q)))
                :where (qu/replace-predicate-fields (:where q) smap)
                :having (qu/replace-predicate-fields (:having q) smap))))]
    [q env]))

(defn ^:private resolve-joined-field [env path]
  (let [[qual basename] (cu/unqualify path)]
    (when-let [qrp (env-get env qual)]
      (when (= :joined-entity (-> qrp :resolved :type))
        (let [jent (-> qrp :resolved :value)
              jfield (dm/field jent basename)]
          (when jfield (r/->ResolvedPath
                         path
                         jent []
                         (r/->Resolved :joined-field jfield)
                         nil)))))))

(defn ^:private resolve-free-path [dm env path]
  (let [[qual basename] (cu/unqualify path)]
    (when-let [qrp (env-get env qual)]
      (when (= :entity (-> qrp :resolved :type))
        (dm/resolve-path dm (-> qrp :resolved :value) basename)))))

(defn resolve-path
  "Attempts to resolve a path against a data model and a query environment.
  Returns a ResolvedPath record or nil."
  [dm ent env path & opts]
  (or
    (when (vector? path)
      (let [[path alias] path]
        (let [rp (apply resolve-path dm ent env path opts)]
          (r/->ResolvedPath alias ent (:chain rp) (:resolved rp) (:shortcuts rp)))))
    (env-get env path 0)
    (when (qu/call? path)
      (r/->ResolvedPath path ent [] (r/->Resolved :call path) nil))
    (when (qu/raw? path)
      (r/->ResolvedPath path ent [] (r/->Resolved :raw path) nil))
    (resolve-joined-field env path)
    (when (vector? env)
      (resolve-free-path dm env path))
    (when-let [[agg-op agg-path] (qu/parse-agg path)]
      (when-let [rp (apply resolve-path dm ent env agg-path opts)]
        (let [resolved (r/->Resolved
                         :agg-op (r/->AggOp agg-op agg-path (:resolved rp)))]
          (r/->ResolvedPath path ent (:chain rp) resolved (:shortcuts rp)))))
    (when (qu/param? path)
      (r/->ResolvedPath path ent [] (r/->Resolved :param path) nil))
    (when dm
      (apply dm/resolve-path dm ent path opts))
    (env-get env path)))

(defn ^:private resolve-and-add-path [dm ent env path & opts]
  (if (or (env-get env path 0) (qu/wildcard? path) (identical? :* path)
          (qu/raw? path) (qu/call? path)
          (number? path) (string? path))
    env
    (if-let [rp (apply resolve-path dm ent env path opts)]
      (let [env (env-assoc env path rp)
            final-path (:final-path rp)
            alias (when (vector? path) (first path))]
        (cond-> env
                (not= final-path path) (env-assoc final-path rp)
                alias (env-assoc alias rp)))
      (throw-info ["Unrecognized path" path "for entity" (:name ent)]
                  {:path path :ename (:name ent) :env env}))))

(defn resolve-and-add-paths
  "Resolves each path in `paths` and adds the mapping to `env`, which is
  returned. Throws an exception if any path does not resolve."
  [dm ent env paths]
  (let [no-fields? (empty? (dm/fields ent))]
    (reduce
      (fn [env path]
        (let [quals (when (and (keyword? path) (not (qu/agg? path)))
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

(defn force-pks
  "If the PK of entity `ent` and any selected rels are not already present in
  query map `eq`, and there are to-many rels referred to, add the PK (or
  multiple PKs) to :select. Returns [q env added-paths]."
  [dm ent env eq]
  (if (not-any? (comp #(some (comp :reverse :rel) %) :chain)
                (vals (immediate-env env)))
    [eq env] ;don't bother if there are no to-many rels
    (let [npk (dm/normalize-pk (:pk ent))
          select (map #(or (:final-path (env-get env %)) %) (:select eq))
          rpks (mapcat
                 (fn [rp]
                   (when-let [rpk (qu/get-rp-pk rp nil)]
                     (let [path (:final-path rp)
                           qual (cu/qualifier path)]
                       (map #(cu/join-path qual %)
                            (dm/normalize-pk rpk)))))
                 (keep #(env-get env %) (remove qu/agg? select)))
          all-pks (distinct (concat npk rpks))
          add-pks (remove (set select) all-pks)
          eq (if (seq add-pks)
               (assoc eq :select (concat select add-pks))
               eq)
          env (resolve-and-add-paths dm ent env add-pks)]
      [eq env add-pks])))

(defn ^:private normalize-query-args [qargs]
  (cond
    (sequential? qargs) qargs
    (map? qargs) [qargs]
    (keyword? qargs) [{:from qargs}]
    :else (throw-info "Invalid query format" {:q qargs})))

(defn expand-query
  "Prepares a query for execution by expanding wildcard fields, implicit joins,
  and subqueries. Returns the transformed query, a map of resolved paths, and
  any added paths: [q env added-paths]."
  [dm qargs & {:keys [expand-joins env force-pk] :or {expand-joins true}}]
  (let [q (apply qb/build-query (normalize-query-args qargs))
        q (if (:select q) q (assoc q :select [:*]))
        from (or (get-from q)
                 (throw-info "No :from found in query" {:q q}))
        ent (dm/entity dm from)
        _ (when-not (and ent (dm/entity? ent))
            (throw-info ["Unrecognized :from -" from] {:q q}))
        q (if (:from q) q (assoc q :from from))
        env (or env (new-env))
        env (env-assoc
              (env-merge env (get-query-env dm ent q env))
              (:name ent) (dm/resolve-path dm ent (:name ent)))
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
        all-fields (qu/get-all-fields q)
        env (resolve-and-add-paths dm ent env all-fields)
        [q env all-fields] (if (false? expand-joins)
                             [q env all-fields]
                             (let [q (expand-implicit-joins q env)
                                   env (get-query-env dm ent q env)
                                   join-fields (qu/get-join-fields q)
                                   env (resolve-and-add-paths
                                         dm ent env join-fields)]
                               [q env (distinct (concat all-fields join-fields))]))
        subqs (filter map? all-fields)
        [q env] (expand-subqueries dm q subqs env)
        [q env added-paths] (if force-pk
                              (force-pks dm ent env q)
                              [q env])]
    [q (immediate-env env) added-paths]))