(ns cantata.core
  "Primary public namespace - data source and data model setup, querying,
  manipulation"
  (:require [cantata.util :as cu :refer [throw-info]]
            [cantata.data-model :as cdm]
            [cantata.data-source :as cds]
            [cantata.records :as r]
            [cantata.query.build :as qb]
            [cantata.query.prepare :as qp]
            [cantata.query.strategy :as qs]
            [cantata.query.nest-results :as qn]
            [cantata.query.util :as qu]
            [cantata.jdbc :as cjd]
            [cantata.parse :as cpa]
            [honeysql.core :as hsql]
            [clojure.java.jdbc :as jd]
            [flatland.ordered.map :as om]))

(defn make-data-model
  "Transforms an entity specification into a DataModel record.

  `name` is an optional keyword name of the data model.

  `entity-specs` can be map, with entity names as keys and maps describing
  the entity as values; or a collection of maps describing the entities.

  Example (map format):

    {:film       {:fields [:id :title]
                  :shortcuts {:actor :film-actor.actor}}
     :actor      {:fields [:id :name]}
     :film-actor {:fields [:film-id :actor-id]
                  :pk [:film-id :actor-id]
                  :rels [:film :actor]}}

  The entity map will be transformed into an Entity record using the
  following keys:

         :name - entity name, a keyword (optional for map format)
           :pk - name(s) of primary key field(s); default: name of first field
       :fields - collection of field specs; see below for format
         :rels - optional collection of rel specs; see below for format
    :shortcuts - optional map of shortcuts; see below for format
     :validate - optional function to validate a entity values map; called
                 prior to inserting or updating; expected to return problem
                 map(s) on validation failure; see `problem`
        :hooks - optional map of hooks; see below for format
      :db-name - string name of corresponding table in SQL database;
                 default: entity name with dashes converted to underscores
    :db-schema - optional string name of table schema in SQL database

  Field specs can be keywords or maps. A keyword is equivalent to a map with
  only the :name key set. The following map keys are accepted:

         :name - field name, a keyword
         :type - optional data type; built-in types:

                   :int :str :boolean :double :decimal :bytes
                   :datetime :date :time

      :db-name - string name of corresponding column in SQL database;
                 default: field name with dashes converted to underscores
      :db-type - optional string type of corresponding column in SQL database

  Relationship (rel) specs can be keywords or maps. A keyword is equivalent to
  a map with only the :name key set. The following map keys are accepted:

         :name - rel name, a keyword
        :ename - name of related entity; default: rel name
          :key - name of foreign key field; default: rel name + \"-id\" suffix
    :other-key - name of referenced key on related entity; default: primary
                 key of related entity
      :reverse - boolean indicating that the foreign key column is on the other
                 table; default: false
          :one - boolean indicating whether the relationship is one-to-one;
                 default: false for reverse rels, true otherwise

  Reverse relationships will be automatically added to related entities
  referenced in rel specs, using the name of the former entity as the rel name.
  If there is more than one reverse relationship created with the same name,
  each will be prefixed with an underscore and the name of the relationship
  to ensure uniqueness, like so: :_rel-name.entity-name.

  Shortcuts take the form of a map of shortcut path to target path. Target
  paths can point to rels or fields.

  Hooks are experimental and may change in future versions. They take the form
  of a map from hook name to hook function. Available hooks and their
  corresponding arguments and expected return values:

    :before-query  [ent expanded-q env added-paths] -> [expanded-q env added-paths]
    :after-query   [ent results] -> results
    :validate      [ent map] -> problems
    :before-save   [ent map] -> map
    :after-save    [ent map ret] -> ret
    :before-insert [ent maps] -> maps
    :after-insert  [ent maps ret] -> ret
    :before-update [ent map pred] -> [map pred]
    :after-update  [ent map pred ret] -> ret
    :before-delete [ent pred] -> pred
    :after-delete  [ent pred ret] -> ret"
  ([entity-specs]
    (make-data-model nil entity-specs))
  ([name entity-specs]
    (cdm/make-data-model name entity-specs)))

(defn data-source
  "Create and return a data source map. The returned map will be compatible
  with both Cantata and clojure.java.jdbc.

            db-spec - a clojure.java.jdbc-compatible spec
       entity-specs - optional DataModel record or entity specs; when provided,
                      entity-specs will transformed into a DataModel; see
                      `make-data-model` for format; will be merged with and
                      take precedence over reflected entity specs

  Keyword options (all default to false unless otherwise noted):

           :reflect - generate a data model from the data source automatically;
                      can be used in combination with entity-specs, the latter
                      taking precedence
            :pooled - create and return a pooled data source
           :init-fn - function to initialize the data source; will be called
                      before reflection and data model creation; the data
                      source map will be passed as the argument
        :joda-dates - return Joda dates for all queries
          :clob-str - convert all CLOB values returned by queries to strings
        :blob-bytes - convert all BLOB values returned by queries to byte
                      arrays
      :ordered-maps - return ordered hash maps for all queries
      :table-prefix - optional table prefix to remove from reflected table names
     :column-prefix - optional column prefix to remove from reflected column
                      names
           :quoting - identifier quoting style to use; auto-detects if
                      left unspecified; set to nil to turn off quoting (this
                      will break many queries); :ansi, :mysql, or :sqlserver
       :query-cache - an atom that wraps a map-like object, such as a cache
                      from clojure.core.cache; used to cache prepared queries;
                      default a 32-element LRU cache
             :hooks - data source-wide hooks; see `make-data-model` for
                      available hooks and format
          :max-idle - max pool idle time in seconds; default 30 mins
   :max-idle-excess - max pool idle time for excess connections, in seconds;
                      default 3 hours

  Wrap your data-source call in `delay` to prevent reflection or pool creation
  from happening at compile time. Cantata will call `force` on the delay when
  it's used."
  [db-spec & entity-specs+opts]
  (apply cds/data-source db-spec entity-specs+opts))

(defn build
  "Builds a query from the given arguments, which can be zero or more maps
  followed by zero or more keyword-value clauses. For example:

    (build {:from :film} :select [:title :actor.name] :limit 1)

  Any maps or clauses after the first will be merged according to the
  semantics of the clause. For example:

    (build {:from :film :select :id :where [:= \"R\" :rating]}
           :select :title :where [:< 90 :length])
    => {:from :film
        :select [:id :title]
        :where [:and [:= \"R\" :rating] [:< 90 :length]]}

  Any paths to related entities referenced outside of :with will trigger outer
  joins when the query is executed. Use the :with clause to trigger an inner
  join instead.

  Supported clauses:

         :from - name of the entity to query; if not provided, will be
                 inferred from :select
       :select - wildcards or paths of fields, relationships, or aggregates to
                 select; unlike SQL, unqualified names will be assumed to refer
                 to the :from entity
        :where - predicate to narrow the result set; see below for format
     :order-by - field names to sort results by; e.g., :title or
                 [[:title :desc] :release-year]
        :limit - integer that limits the number of results
       :offset - integer offset into result set
     :group-by - fields to group results by; forbidden for certain multi-queries
       :having - like :where but performed after :group-by
    :modifiers - one or more keyword modifiers:
                   :distinct - return distinct results
      :include - one or more relationship names to perform a left outer join
                 with. May also be a map of the form:
                 {:rel-name [:foo :bar :baz]}, to select specific fields from
                 related entities.
         :with - like :include but performs an inner join
      :without - return results that have no related entity records for the
                 provided relationship names
         :join - explicit inner join; e.g., [[:foo :f] [:= :id :f.id]]
    :left-join - explicit left outer join
      :options - a map with the following optional keys:
                   :join-type - whether to perform an :outer (the default) or
                                :inner join for fields selected from related
                                entities

  Where applicable, there are variants of each clause with a :replace- or :un-
  prefix. For example, :replace-select will replace, rather than merge, the
  :select clause; and :un-select will remove fields from :select.

  Predicates are vectors of the form [op arg1 arg2 ...], where args are
  paths, other predicates, etc. Built-in ops:

    :and :or :xor :not
    := :not= :< :<= :> :>=
    :in :not-in :like :not-like :between
    :+ :- :* :/ :% :mod :| :& :^

  Example predicate: [:and [:= \"Drama\" :category.name] [:< 90 :length 180]]

  Aggregates are keywords that begin with % - e.g., :%count.actor.id

  Bindable parameters are denoted with a leading ? - e.g., :?actor-name"
  [& qargs]
  (apply qb/build-query (if (vector? (first qargs))
                          (first qargs)
                          qargs)))

(defn query-meta
  "Returns information about query results gathered before the query was
  executed, if available. The returned map will have keys:

           :from - entity queried
            :env - map of resolved paths from the query
       :expanded - expanded form of the query
    :added-paths - paths added by Cantata to the query before executing it

  Query meta data is normally attached to either the collection of maps
  returned, or to the collection of column names returned."
  ([results]
    (:cantata/query (meta results)))
  ([results k]
    (k (:cantata/query (meta results)))))

(defn prepare-query
  "Return a PreparedQuery record, which contains ready-to-execute SQL and
  other necessary meta data. When executed, the query will accept bindable
  parameters. (Bindable paramters can be included in queries using keywords
  like :?actor-name.)

  Unlike a JDBC PreparedStatement, a PreparedQuery record contains no
  connection-specific information and can be reused at any time.

  Query preparation may be cached if the data source has been configured to
  do so (see `data-source`)."
  [ds q & opts]
  (if (qu/prepared? q)
    q
    (let [opts (concat opts [:quoting (cds/get-quoting ds)])] ;cacheable?
      (if-let [qcache (cds/get-query-cache ds)]
        (if-let [e (find @qcache q)] ;cached?
          (val e)
          (let [pq (apply qp/prepare-query ;not cached
                          (cds/get-data-model ds) q opts)]
            (swap! qcache assoc q pq)
            pq))
       (apply qp/prepare-query ;not cacheable
              (cds/get-data-model ds) q opts)))))

(defn with-query-rows*
  "Low level querying function that returns rows and cols. All querying
  funnels through here."
  ([ds q body-fn]
    (with-query-rows* ds q nil body-fn))
  ([ds q opts body-fn]
    (let [dm (cds/get-data-model ds)
          opts (if (map? opts)
                 (apply concat opts)
                 opts)
          pq (apply prepare-query ds q opts)
          ret (apply cjd/query (force ds) dm pq body-fn opts)
          qmeta (query-meta (if (vector? (first ret))
                              (first ret)
                              ret))]
      (if-let [from-ent (:from qmeta)]
        (cds/maybe-invoke-hook ret ds from-ent :after-query ret)
        ret))))

(defmacro with-query-rows
  "Evaluates body after executing a query against a data source and binding
  column names and row results to the symbols designated by `cols` and `rows`,
  respectively. See `build` for query format."
  [cols rows ds q & body]
  `(with-query-rows* ~ds ~q (fn [~cols ~rows]
                              ~@body)))

(defn with-query-maps*
  "Helper function for with-query-maps"
  ([ds q body-fn]
    (with-query-maps* ds q nil body-fn))
  ([ds q opts body-fn]
    (let [ds-opts (cds/get-options ds)]
      (with-query-rows* ds q opts
        (fn [cols rows]
          (body-fn (with-meta
                     (map #(qu/build-result-map cols % ds-opts)
                          rows)
                     (meta cols))))))))

(defmacro with-query-maps
  "Evaluates body after executing a query against a data source and binding
  result maps to the symbol designated by `maps`. See `build` for query format."
  [maps ds q & body]
  `(with-query-maps* ~ds ~q (fn [~maps]
                              ~@body)))

;; TODO: public version that works on arbitrary vector/map data, sans meta data
(defn ^:private nest-results
  ([cols rows & {:keys [ds-opts]}]
    (let [{:keys [from env added-paths]} (query-meta cols)
          opts (merge ds-opts
                      {:added-paths (set added-paths)})]
      (with-meta
        (qn/nest-results cols rows from env opts)
        (meta cols)))))

(defn ^:private flat-query [ds q & [{:keys [vectors] :as opts}]]
  (if vectors
    (with-query-rows* ds q opts
      (fn [cols rows]
        [cols rows]))
    (with-query-maps* ds q opts identity)))

(declare query)

(defn ^:private multi-query
  [ds q opts]
  (when (:flat opts)
    (throw-info "Cannot execute flat queries with :strategy :multiple" {:q q}))
  (when (:vectors opts)
    (throw-info "Cannot execute vector queries with :strategy :multiple" {:q q}))
  (when (qu/plain-sql? q)
    (throw-info "Cannot execute plain SQL queries with :strategy :multiple" {:q q}))
  (when (qu/prepared? q)
    (throw-info "Cannot execute prepared queries with :strategy :multiple; use query instead" {:q q}))
  (let [dm (cds/get-data-model ds)
        opts* (apply concat (dissoc opts :strategy))
        query-fn (fn [q & opts]
                   (apply query ds q opts*))]
    (apply qs/multi-query query-fn dm q opts*)))

(defn query
  "Executes query `q` against data source `ds`. The query can be one of the
  following:

     * Query map/vector - see `build` for format
     * PreparedQuery record - see `prepare-query`
     * SQL string
     * clojure.java.jdbc-style [sql params] vector

  By default, returns a sequence of maps, with nested maps and sequences
  for values selected from related entities. Example:

    (query ds {:from :film :select [:title :actor.name] :where [:= 1 :id]})
    => [{:title \"Lawrence of Arabia\"
         :actor [{:name \"Peter O'Toole\"} {:name \"Omar Sharif\"}]}]

  NOTE: using the :limit clause may truncate nested values from to-many
  relationships. To limit your query to a single top-level entity record while
  retrieving all related records, restrict the results using the :where clause,
  or set the :strategy option to :multiple.

  Keyword options:

    :strategy - fetching strategy to use:
                  :single   - (default) fetches all data in a single round trip
                  :multiple - fetches data in multiple round trips, one for
                              each selected second-level path segment that is
                              part of a path that contains a to-many
                              relationship. Primary keys of the top-level
                              entity results are used to fetch the related
                              results. :limit and :where clauses apply only to
                              the top-level entity query.
        :flat - do not nest results; results for the same primary key may be
                returned multiple times if the query selects paths from any
                to-many relationships
     :vectors - return results as a vector of [cols rows], where cols is a
                vector of column names, and rows is a sequence of vectors with
                values for each column
      :params - map of bindable param names to values
    :force-pk - prevent Cantata from implicitly adding primary keys to the
                the low-level database query when to-many relationships are
                selected (which it does to make nesting more predictable and
                consistent)"
  [ds q & {:keys [strategy flat vectors force-pk] :as opts}]
  (if (= :multiple strategy)
    (multi-query ds q opts)
    (if (or flat vectors (qu/plain-sql? q))
      (flat-query ds q opts)
      (with-query-rows* ds q (assoc opts :force-pk (not (false? force-pk)))
        (fn [cols rows]
          (nest-results cols rows :ds-opts (cds/get-options ds)))))))

(defn query1
  "Like `query` but returns the first result. If :strategy is :multiple, limits
  the query to a single result; otherwise, does not limit the query, so it's
  the responsbility of the caller to not query for more results than needed."
  [ds q & opts]
  (let [optsm (apply hash-map opts)
        q (if (= :multiple (:strategy optsm))
            (qu/add-limit-1 q)
            q)
        ret (apply query ds q opts)]
    (if (:vectors optsm)
      (when-let [ret1 (first (second ret))]
        (with-meta ret1 (meta (first ret))))
      (when-let [ret1 (first ret)]
        (with-meta ret1 (meta ret))))))

(defn query-count
  "Returns the number of matching results for query `q`. By default, returns
  the count of distinct top-level entity results. Set the :flat option to true
  to return the count of ALL rows, including to-many rows with redundant
  top-level values."
  [ds q & opts]
  (let [opts (concat opts [:quoting (cds/get-quoting ds)
                           :vectors true])
        pq (apply prepare-query ds q opts)
        pqc (apply qp/prepare-count-query (cds/get-data-model ds) pq opts)
        [_ rows] (apply query ds pqc opts)]
    (ffirst rows)))

(defn by-id
  "Fetches the entity record from the data source whose primary key value is
  equal to `id`. Query clauses from `q` will be merged into the generated
  query."
  [ds ename id & [q & opts]]
  (let [ent (cdm/entity (cds/get-data-model ds) ename)
        [q opts] (if (keyword? q)
                   [nil (cons q opts)]
                   [q opts])
        baseq {:from ename :where (qb/build-key-pred (:pk ent) id)}]
    (apply query1 ds (if (map? q)
                       [baseq q]
                       (cons baseq q))
           opts)))

(defn getf
  "Returns one or more nested field values from the given query result or
  results, traversing into related results as necessary, according to a
  dotted keyword path.

  `path` and `qr` can be swapped as arguments. The following calls do the same
  thing:

    (getf :actor.name results)
    (getf results :actor.name)
  
  If invoked with one argument, returns a partial function."
  ([path]
    #(getf % path))
  ([qr path]
    (qu/getf qr path)))

(defn getf1
  "Returns a nested field value from the given query result or results,
  traversing into related results as necessary. Returns the same as `getf`
  except if the result would be a sequence, in which case it returns the first
  element.

  `path` and `qr` can be swapped as arguments. The following calls do the same
  thing:

    (getf1 :actor.name results)
    (getf1 results :actor.name)

  If invoked with one argument, returns a partial function."
  ([path]
    #(getf1 % path))
  ([qr path]
    (let [ret (getf qr path)]
      (if (sequential? ret)
        (first ret)
        ret))))

(defn queryf
  "Same as `query`, but additionally calls `getf` using the first selected path.
  Example:

    (queryf ds {:from :film :select :actor.name :where [:= 1 :id]})
    => [\"Peter O'Toole\" \"Omar Sharif\"]"
  [ds q & opts]
  (let [res (apply query ds q opts)
        env (query-meta res :env)
        f1name (qu/first-select-field q)]
    (if (= f1name (get-in env [f1name :root :name]))
      res
      (getf res (get-in env [f1name :final-path])))))

(defn queryf1
  "Same as query, but additionally calls getf1 using the first selected path.
  Example:

    (queryf1 ds {:from :film :select :actor.name :where [:= 1 :id]})
    => \"Peter O'Toole\""
  [ds q & opts]
  (first (apply queryf ds q opts)))

;;;;

(defn to-sql
  "Returns a clojure.java.jdbc-compatible [sql params] vector for the given
  query."
  [ds-or-dm q & opts]
  (let [[ds dm] (if (cdm/data-model? ds-or-dm)
                  [nil ds-or-dm]
                  [ds-or-dm nil])
        opts (concat opts [:force-pk false])
        optsm (apply hash-map opts)]
    (if ds
      (cjd/populate-sql-params
        ds (apply prepare-query ds q opts) (:params optsm))
      (apply qp/to-sql
             q
             :data-model dm
             opts))))

;;;;

(defmacro with-transaction
  "A light wrapper around clojure.java.jdbc/with-db-transaction.

  Begins a transaction, binding the connection-bearing db-spec to a given
  symbol. If given a single symbol as the binding, creates a shadowing binding
  with the same name."
  [binding & body]
  (let [[ds-sym ds] (if (symbol? binding)
                      [binding binding]
                      binding)]
    `(jd/with-db-transaction [~ds-sym (force ~ds)] ~@body)))

(defmacro with-connection
  "Opens a connection, binding the connection-bearing db-spec to a given
  symbol. If given a single symbol as the binding, creates a shadowing binding
  with the same name."
  [binding & body]
  (let [[ds-sym ds] (if (symbol? binding)
                      [binding binding]
                      binding)]
    `(let [ds# (force ~ds)]
       (with-open [con# ^java.sql.Connection (jd/get-connection ds#)]
         (let [~ds-sym (jd/add-connection ds# con#)]
           ~@body)))))

(defmacro rollback!
  "Signals that the current transaction should roll back on completion"
  [ds]
  `(jd/db-set-rollback-only! ~ds))

(defmacro unset-rollback!
  "Signals that the current transaction should NOT roll back on completion"
  [ds]
  `(jd/db-unset-rollback-only! ~ds))

(defmacro rollback?
  "Returns true if the current transaction is set to roll back on completion"
  [ds]
  `(jd/db-is-rollback-only ~ds))

(defmacro with-rollback
  "Begins a transaction that is immediately set to roll back on completion"
  [binding & body]
  (let [[ds-sym ds] (if (symbol? binding)
                      [binding binding]
                      binding)]
    `(with-transaction [~ds-sym ~ds]
       (rollback! ~ds-sym)
       ~@body)))

(defmacro verbose
  "Causes any wrapped queries or statements to print lots of low-level database
  logging information during execution. See also `with-debug`"
  [& body]
  `(binding [cu/*verbose* true]
     ;; Hack
     (with-redefs [jd/db-do-prepared cjd/db-do-prepared-hook
                   jd/db-do-prepared-return-keys cjd/db-do-prepared-return-keys-hook]
       ~@body)))

(defmacro with-debug
  "Starts a transaction which will be rolled back on completion, and prints
  verbose information about all database activity.

  WARNING: The underlying data source must actually support rollbacks. If it
  doesn't, changes may be permanently committed."
  [binding & body]
  `(verbose
     (with-rollback ~binding
       ~@body)))

;;;;

(defmacro ^:private def-dm-helpers [& fn-names]
  `(do
     ~@(for [fn-name fn-names]
         (let [fn-sym (symbol "cdm" (name fn-name))
               fn-meta (meta (resolve fn-sym))]
           `(defn ~fn-name ~(:doc fn-meta "") [~'ds ~'& ~'args]
              (apply ~fn-sym
                     (cds/get-data-model ~'ds) ~'args))))))

(def-dm-helpers ;being lazy
  entities entity rels rel fields field field-names shortcut shortcuts
  resolve-path)

(defn data-model
  "Return the DataModel record bundled within a data source"
  [ds]
  (cds/get-data-model ds))

(defn parse
  "Parses a map or sequence of field names and corresponding values into an
  entity values map, using field types to guide parsing. Takes data source
  options (such as Joda dates) into consideration."
  ([ds ename-or-ent values]
    (let [dm (cds/get-data-model ds)
          ent (if (keyword? ename-or-ent)
                (or (cdm/entity dm ename-or-ent)
                    (throw-info ["Unrecognized entity" ename-or-ent]
                                {:ename ename-or-ent}))
                ename-or-ent)
          [fnames values] (if (map? values)
                            [(keys values) (vals values)]
                            [(cdm/field-names ent) values])]
      (parse ds ent fnames values)))
  ([ds ename-or-ent fnames values]
    (let [dm (cds/get-data-model ds)
          ent (if (keyword? ename-or-ent)
                (or (cdm/entity dm ename-or-ent)
                    (throw-info ["Unrecognized entity" ename-or-ent]
                                {:ename ename-or-ent}))
                ename-or-ent)
          joda-dates? (cds/get-option ds :joda-dates)]
      (cpa/parse ent fnames values :joda-dates joda-dates?))))

(defn validate!
  "Invoke the :validate hook on the entity, passing it the given map of values.
  If validation fails (i.e., the validation function returns problem maps),
  throws an ExceptionInfo instance with the :problems and :values keys set."
  [ds ename-or-ent m]
   (let [ent (if (keyword? ename-or-ent)
               (cdm/entity (cds/get-data-model ds) ename-or-ent)
               ename-or-ent)]
     (let [problems (cds/maybe-invoke-hook nil ds ent :validate m)]
       (when (and problems (not (and (sequential? problems)
                                     (empty? problems))))
         (throw-info ["Validation failed for entity" (:name ent)]
                     {:problems (cu/seqify problems) :values m})))))

(defn problem
  "Creates and returns a problem map with optional keys and msg. Validation
  functions can use this to describe validation problems."
  ([keys-or-msg]
    (let [[keys msg] (if (sequential? keys-or-msg)
                        [keys-or-msg "Invalid value"]
                        [nil keys-or-msg])]
      (problem keys msg)))
  ([keys msg]
    (r/->ValidationProblem keys msg)))

;;;;

(defn ^:private get-own-map
  "Returns a map with only the fields of the top-level entity - no related
  fields."
  [ent m & {:as opts}]
  (let [fnames (cdm/field-names ent)]
    (if (seq fnames)
      (select-keys m (cdm/field-names ent))
      (select-keys m (remove cu/first-qualifier (keys m))))))

(defn ^:private prep-map [ds ent m validate?]
  (let [m (->> m
            (get-own-map ent)
            (cpa/marshal ent))]
    (when-not (false? validate?)
      (validate! ds ent m))
    m))

(defn insert!
  "Inserts an entity map or maps into the data source. Unlike `save!`, the
  maps may not include related entity maps.

  By default, returns the primary key(s) of the inserted maps.

  Multiple maps will be inserted in a transaction.

  Options:

    :validate    - false to prevent validation.
    :return-keys - false for more efficient insertion of multiple records
                   (using batches) but without returning generated keys"
  [ds ename m-or-ms & {:keys [validate return-keys]}]
  (let [ms (cu/seqify m-or-ms)
        dm (cds/get-data-model ds)
        ent (or (cdm/entity dm ename)
                (throw-info ["Unrecognized entity" ename]
                            {:ename ename}))
        ms* (map #(prep-map ds ent % validate) ms)
        ms* (cds/maybe-invoke-hook ms* ds ent :before-insert ms*)]
    (let [ret-keys (cjd/insert! (force ds) dm ename ms* :return-keys return-keys)
          ret (if (sequential? m-or-ms)
                ret-keys
                (first ret-keys))]
      (cds/maybe-invoke-hook ret ds ent :after-insert ms* ret))))

(defn update!
  "Updates data source records that match the given predicate with the given
  values. Unlike save, no related field values can be included.

  Set :validate to false to prevent validation.

  Returns the number of records affected by the update."
  [ds ename values pred & {:keys [validate]}]
  (let [dm (cds/get-data-model ds)
        ent (or (cdm/entity dm ename)
                (throw-info ["Unrecognized entity" ename]
                            {:ename ename}))
        values* (prep-map ds ent values validate)
        [values* pred] (cds/maybe-invoke-hook
                         [values* pred]
                         ds ent :before-update values* pred)]
    (let [ret (cjd/update! (force ds) dm ename values* pred)]
      (cds/maybe-invoke-hook ret ds ent :after-update values* pred ret))))

(defn delete!
  "Deletes records from data source that match the given predicate, or all
  records if no predicate is provided.

  Returns the number of records deleted."
  ([ds ename]
    (delete! ds ename nil))
  ([ds ename pred & {:as opts}]
    (let [dm (cds/get-data-model ds)
          ent (or (cdm/entity dm ename)
                  (throw-info ["Unrecognized entity" ename]
                              {:ename ename}))
          pred (cds/maybe-invoke-hook pred ds ent :before-delete pred)]
      (let [ret (cjd/delete! (force ds) dm ename pred)]
        (cds/maybe-invoke-hook ret ds ent :after-delete pred ret)))))

(defn cascading-delete!
  "Deletes ALL database records for an entity, and ALL dependent records. Or,
  if no entity is given, deletes ALL records for ALL entities in the data
  model. BE CAREFUL!"
  ([ds]
    (let [dm (cds/get-data-model ds)]
      ;; Doesn't keep track of which have already been deleted, so will
      ;; delete entities multiple times. Oh well.
      (doseq [ent (cdm/entities dm)]
        (cascading-delete! ds (:name ent)))))
  ([ds ename]
    (let [dm (cds/get-data-model ds)
          deps (cdm/dependent-graph dm)]
      (doseq [[dep-name] (deps ename)]
        (cascading-delete! ds dep-name))
      (delete! ds ename))))

(defn delete-ids!
  "Deletes records from data source that match the given ids.

  Returns the number of records deleted."
  [ds ename id-or-ids & {:as opts}]
  (let [dm (cds/get-data-model ds)
        ent (cdm/entity dm ename)
        ids (cu/seqify id-or-ids)]
    (delete! ds ename (qb/build-key-pred (:pk ent) ids))))

(defn cascading-delete-ids!
  "Deletes any dependent records and then deletes the entity record for the
  given id"
  [ds ename id-or-ids]
  (let [dm (cds/get-data-model ds)
        ent (cdm/entity dm ename)
        pk (:pk ent)
        npk (cdm/normalize-pk pk)
        deps (cdm/dependent-graph dm)]
    (doseq [[dep-name dep-rel] (deps ename)]
      (let [dep-ent (cdm/entity dm dep-name)
            dep-pk (:pk dep-ent)
            other-rk (vec (for [k (cdm/normalize-pk (:other-key dep-rel))]
                            (cu/join-path (:name dep-rel) k)))
            other-pk (vec (for [k npk]
                            (cu/join-path (:name dep-rel) k)))
            del-ms (flat-query
                     ds [:from dep-name
                         :select (cdm/normalize-pk dep-pk)
                         :where [:and
                                 (qb/build-key-pred (:key dep-rel) other-rk)
                                 (qb/build-key-pred other-pk id-or-ids)]])]
        (doseq [del-m del-ms]
          (cascading-delete-ids! ds dep-name (cdm/pk-val del-m dep-pk))))))
  (delete-ids! ds ename id-or-ids))

(defn execute!
  "Execute arbitrary SQL or Cantata query. Light wrapper around
  clojure.java.jdbc/execute!"
  [ds q]
  (jd/execute! (force ds) (to-sql ds q)))

;;;;

(declare save!)

(defn ^:private save-m [ds ent m & opts]
  (let [pk (:pk ent)
        id (cdm/pk-val m pk)
        ename (:name ent)
        optsm (apply hash-map opts)]
    (if (nil? id)
      (apply insert! ds ename m opts)
      (do
        (if (or (false? (:check-update optsm))
                (first (flat-query
                         ds [:from ename :select pk :where [:= pk id]])))
          (apply update! ds ename m [:= pk id] opts)
          (apply insert! ds ename m opts))
        id))))

(defn ^:private assoc-pk [m pk id]
  (if (sequential? pk)
    (reduce
      (fn [m [pk id]]
        (assoc m pk id))
      m
      (map list pk id))
    (assoc m pk id)))

(defn ^:private save-belongs-to [ds dm rel rms ent m]
  (let [rent (cdm/entity dm (:ename rel))
        rm (first rms) ;one-to-one
        rpk (:pk rent)
        nrpk (cdm/normalize-pk rpk)
        rid (when (seq rm)
              (if (and (= (count nrpk) (count rm))
                       (cdm/pk-present? rm rpk))
                (cdm/pk-val rm rpk)
                (save! ds (:name rent) rm)))
        pk (:pk ent)]
    (assoc-pk m (:key rel) rid)))

(defn ^:private save-has-many-via [ds dm rels rms ent id]
  (let [[via-rel rel] rels
        vent (cdm/entity dm (:ename via-rel))
        rent (cdm/entity dm (:ename rel))
        vfk (:other-key via-rel)
        vpk (:pk vent)
        vrfk (:key rel)
        old-vms (flat-query
                  ds [:from (:name vent)
                      :select [vpk vrfk]
                      :where (qb/build-key-pred vfk id)])
        old-rids (map #(cdm/pk-val % vrfk) old-vms)
        rids (doall
              (for [rm rms]
                (save! ds (:name rent) rm)))
        rem-rids (remove (set rids) old-rids)
        add-rids (remove (set old-rids) rids)]
    (doseq [rid add-rids]
      (save! ds (:name vent) (-> {}
                               (assoc-pk vfk id)
                               (assoc-pk vrfk rid))))
    ;; Only recs in the junction table are removed; NOT from the rel table
    (when (seq rem-rids)
      (delete! ds (:name vent) (qb/build-key-pred
                                 [vfk vrfk]
                                 (map vector (repeat id) rem-rids))))
    true))


(defn ^:private save-has-many [ds dm rels rms ent id]
  (if (< 1 (count rels))
    (save-has-many-via ds dm rels rms ent id)
    (let [rel (first rels)
          rent (cdm/entity dm (:ename rel))
          rpk (:pk rent)
          fk (or (:other-key rel) rpk)
          rename (:name rent)
          old-rms (flat-query
                    ds [:from rename :select rpk :where (qb/build-key-pred fk id)])
          rids (doall
                (for [rm rms]
                  (save! ds (:name rent) (assoc-pk rm fk id))))
          rem-rids (remove (set rids)
                           (map #(cdm/pk-val % rpk) old-rms))]
      (when (seq rem-rids)
        (delete-ids! ds rename rem-rids))
      true)))

(defn ^:private rel-save-allowed? [chain]
  ;; Must be a belongs-to or has-many relationship
  (or (= 1 (count chain))
      (and (= 2 (count chain))
           (let [[l1 l2] chain]
             (and (-> l1 :rel :reverse)
                  (not (-> l2 :rel :reverse)))))))

(defn ^:private normalize-rel-map [ent m]
  (if (map? m)
    m
    (if (vector? m)
      (zipmap (:pk ent) m)
      {(:pk ent) m})))

(defn ^:private get-rel-maps [dm ent m]
  (let [rels (cdm/rels ent)]
    (reduce-kv
      (fn [rms k v]
        (let [rp (cdm/resolve-path dm ent k)
              chain (not-empty (:chain rp))]
          (if-not chain
            rms
            (if-not (rel-save-allowed? chain)
              (throw-info
                ["Rel" k "not allowed here - saving entity" (:name ent)]
                {:rname k :ename (:name ent)})
              (let [rent (:to (peek chain))]
                (assoc rms (mapv :rel chain)
                       (map #(normalize-rel-map rent %)
                            (cu/seqify v))))))))
      {} m)))

(defn ^:private save-m-rels [ds dm ent own-m m & opts]
  (let [rel-ms (get-rel-maps dm ent m)
        [fwd-rel-ms rev-rel-ms] ((juxt filter remove)
                                  #(-> % key first :reverse not)
                                  rel-ms)
        own-m (reduce
                (fn [own-m [[rel] rms]]
                  (save-belongs-to ds dm rel rms ent own-m))
                own-m
                fwd-rel-ms)
        id (apply save-m ds ent own-m opts)]
    (doseq [[rels rms] rev-rel-ms]
      (save-has-many ds dm rels rms ent id))
    id))

(defn save!
  "Saves entity map values to the database. If the primary key is included in
  the map, the DB record will be updated if it exists. If it doesn't exist, or
  if no primary key is provided, a new record will be inserted.

  The map may include nested, related maps, which will in turn be saved and
  associated with the top-level record. Nested rel values may also be bare
  primary key values instead of maps.

  All changes happen within a transaction.

  Values will be marshalled before being sent to the database. Joda dates,
  for example, will be converted to java.sql dates.

  Returns the primary key of the updated or inserted record.

  Accepts the following keyword options:
      :validate     - when false, does not validate saved records
      :check-update - when false, assumes presence of PK = update
      :save-rels    - when false, does not save related records"
  [ds ename values & {:keys [validate save-rels] :as opts}]
  (let [dm (cds/get-data-model ds)
        ent (or (cdm/entity dm ename)
                (throw-info ["Unrecognized entity" ename]
                            {:ename ename}))
        values (cds/maybe-invoke-hook values ds ent :before-save values)
        own-m (get-own-map ent values)]
    (let [ret (with-transaction ds
                (if (false? save-rels)
                  (apply save-m ds ent own-m (apply concat opts))
                  (apply save-m-rels ds dm ent own-m values (apply concat opts))))]
      (cds/maybe-invoke-hook ret ds ent :after-save values ret))))

;;;;

(defn merge-and-delete!
  "Updates all records that point to id-to-merge, merges any values
  not already present in id-to-keep, then deletes id-to-merge."
  [ds ename id-to-keep id-to-merge]
  (when-let [keep-m (merge
                      (by-id ds ename id-to-merge)
                      (by-id ds ename id-to-keep))]
    (save! ds ename keep-m)
    (let [dm (cds/get-data-model ds)
          ent (or (cdm/entity dm ename)
                  (throw-info ["Unrecognized entity" ename]
                              {:ename ename}))
          pk (:pk ent)
          npk (cdm/normalize-pk pk)
          deps (cdm/dependent-graph dm)]
      (doseq [[dep-name dep-rel] (deps ename)]
        (let [dep-ent (cdm/entity dm dep-name)
              dep-pk (:pk dep-ent)
              other-rk (vec (for [k (cdm/normalize-pk (:other-key dep-rel))]
                              (cu/join-path (:name dep-rel) k)))
              other-pk (vec (for [k npk]
                              (cu/join-path (:name dep-rel) k)))
              mod-ms (flat-query
                       ds [:from dep-name
                           :select (cdm/normalize-pk dep-pk)
                           :where [:and
                                   (qb/build-key-pred (:key dep-rel) other-rk)
                                   (qb/build-key-pred other-pk id-to-merge)]])]
          (doseq [mod-m mod-ms]
            (let [dep-id (cdm/pk-val mod-m dep-pk)]
              (if (= dep-pk (:key dep-rel))
                (merge-and-delete! ds dep-name id-to-keep dep-id)
                (update! ds dep-name
                         (assoc-pk {} (:key dep-rel) id-to-keep)
                         (qb/build-key-pred dep-pk dep-id))))))))
    (delete-ids! ds ename id-to-merge)))

;;;;

(defn raw
  "Returns a raw SQL string fragment. Using this in queries could cause
  strange behavior."
  [s]
  (qu/raw s))

(defn call
  "Returns a SQL call for embedding in queries; fn-name should be a keyword"
  [fn-name & args]
  (apply qu/call fn-name args))