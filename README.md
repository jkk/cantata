# Cantata

Melodic SQL for Clojure. Highlights:

* Pure-data queries
* Relationship-aware querying and saving
* Legacy-friendly: custom name mappings, composite primary keys
* Extensibile

## Installation

Leiningen coordinate:

```clj
[cantata "0.1.0"]
```

## Crash Course

See the [Quick Reference](#quick-reference) for a more systematic breakdown.

To use Cantata, first you need a data source and a data model. Let's assume we have a schema like this:

![schema](https://github.com/jkk/cantata/raw/master/doc/simplified_schema.png)

To get up and running quickly, you can let Cantata work out most of the data model itself using reflection:

```clj
(ns example.core
  (:require [cantata.core :as c]))

;; Any clojure.java.jdbc compatible DB spec
(def mysql-spec "jdbc:mysql://localhost/film_store")

;; Shortcuts to supplement the reflected model
(def model
  {:film {:shortcuts {:actor :film-actor.actor
                      :category :film-category.category
                      :renter :inventory.rental.customer}}
   :customer {:shortcuts {:country-name :address.city.country.country}}})

(def ds (delay (c/data-source
                 mysql-spec model
                 :reflect true)))
```
Note that Cantata does not create database tables or do migrations. We're merely glomming onto a schema that has been created elsewhere.

### Querying

Cantata leverages the data model to perform queries that fetch and combine data from any number of related tables. The following query fetches the film with id 1, plus related language, category, and actor data -- all in one database round trip, and nested nicely:

```clj
(c/query ds [:from :film
             :select [:id :title :release-year]
             :include [:language :category :actor]
             :where [:= 1 :id]])

=> [{:id 1
     :title "ACADEMY DINOSAUR"
     :release-year 2006
     :language {:name "English", :id 1}
     :category [{:name "Documentary", :id 6}]
     :actor [{:name "PENELOPE GUINESS", :id 1}
             {:name "CHRISTIAN GABLE", :id 10}
             {:name "LUCILLE TRACY", :id 20}]}]
```

Queries are made entirely of simple data, and can be easily amended on the fly:

```clj
(def kid-film {:from :film
               :where [:and
                       [:in :rating ["G" "PG"]]
                       [:< 90 :length 100]]})

(c/query ds [kid-film :select [:title :release-year] :limit 3])

=> [{:title "ARMAGEDDON LOST" :release-year 2006}
    {:title "BILL OTHERS" :release-year 2006}
    {:title "BOUND CHEAPER" :release-year 2006}]
```

You can refer to related entities anywhere in a query, and Cantata will work out the joins for you:

```clj
;; 8 joins against 9 tables - one round trip
(c/query ds
         [:from :film
          :select [:title :actor.name]
          :where [:= "Canada" :renter.country-name]])
```

You can have Cantata fetch data from related tables in multiple database round trips if you prefer, using `querym`. With this strategy, primary keys gathered during an initial query will be used to find data from related tables. Both fetching strategies -- single vs. multiple round trips -- have benefits and costs. Cantata lets choose.

```clj
;; 3 database round trips - one for each to-many relationship
(c/querym ds [:from :film
              :include [:language :category :actor]
              :limit 10])
```

### Saving & Deleting

Cantata can also leverage the data model when saving. Here we add a new film with relevant categories attached:

```clj
;; Returns the generated id
(c/save! film {:title "Lawrence of Arabia"
               :language-id 1
               :category [{:id 7}            ;Drama
                          {:id 4}            ;Classics
                          {:name "Epic"}]})  ;Doesn't exist - will be created
```
Saving the film record and all related records happens within a transaction.

If the primary key of a record is present, an update will be performed:

```clj
;; Update a film - affects only the fields provided
(c/save! film {:film-id 1001 :release-year 1962})
```

## Playground Project

A playground project, which uses a fully fleshed out version of the movie store schema, with lots of fake data, is available in the __[cantata-sample](https://github.com/jkk/cantata-sample)__ repo.

## Quick Reference

### Data Model

A data model describes the entities in a system and the relationships between them. A data model exists independently of a data source, but they are bundled for convenience.

Cantata can inspect a data source and generate a data model for you automatically; see the `data-source` function's `:reflect` option. The following describes how to define a data model explicitly. Both methods can be used together, with your explicit definitions taking precedence over the reflected ones.

A data model is created using the `data-source` or `make-data-model` functions, which take entity specs. Cantata transforms the entity specs you provide into a `DataModel` record internally.

Entity specs can be either a map, with entity names as keys and maps describing
the entity as values; or a collection of maps describing the entities.

Example (map format):

    {:film       {:fields [:id :title]
                  :shortcuts {:actor :film-actor.actor}}
     :actor      {:fields [:id :name]}
     :film-actor {:fields [:film-id :actor-id]
                  :pk [:film-id :actor-id]
                  :rels [:film :actor]}}

Entity descriptor maps can contain the following keys:

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
only the `:name` key set. The following map keys are accepted:

         :name - field name, a keyword
         :type - optional data type; built-in types:

                   :int :str :boolean :double :decimal :bytes
                   :datetime :date :time

      :db-name - string name of corresponding column in SQL database;
                 default: field name with dashes converted to underscores
      :db-type - optional string type of corresponding column in SQL database

Relationship (rel) specs can be keywords or maps. A keyword is equivalent to
a map with only the `:name` key set. The following map keys are accepted:

         :name - rel name, a keyword
        :ename - name of related entity; default: rel name
          :key - name of foreign key field; default: rel name + "-id" suffix
    :other-key - name of referenced key on related entity; default: primary
                 key of related entity
      :reverse - boolean indicating whether the foreign key is on this or
                 the other table

Reverse relationships will be automatically added to related entities
referenced in rel specs, using the name of the former entity as the rel name.
If there is more than one reverse relationship created with the same name,
each will be prefixed with an underscore and the name of the relationship
to ensure uniqueness, like so: `:_rel-name.entity-name`.

Shortcuts take the form of a map of shortcut path to target path. Target
paths can point to rels or fields.

Hooks take form of a map from hook name to hook function. Available hooks:

    :validate :before-save :after-save :before-update :after-update
    :before-delete :after-delete
  
### Data Source

A data source is where data comes from and gets stored to: a database.

A data source map is created using the `data-source` function. The map it returns will be compatible with both Cantata and clojure.java.jdbc.

`data-source` takes two arguments: `db-spec` and `entity-specs`, plus keyword options.

            db-spec - a clojure.java.jdbc-compatible spec
       entity-specs - optional DataModel record or entity specs; when provided,
                      entity-specs will transformed into a DataModel; will be
                      merged with and take precedence over reflected entity specs

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
     :unordered-maps - return unordered hash maps for all queries
       :table-prefix - optional table prefix to remove from reflected table names
      :column-prefix - optional column prefix to remove from reflected column
                       names
            :quoting - identifier quoting style to use; auto-detects if
                       left unspecified; set to nil to turn off quoting (this
                       will break many queries); :ansi, :mysql, or :sqlserver
           :max-idle - max pool idle time in seconds; default 30 mins
    :max-idle-excess - max pool idle time for excess connections, in seconds;
                       default 3 hours

Wrap your data-source call in `delay` to prevent reflection or pool creation
from happening at compile time. Cantata will call `force` on the delay when
it's used.

### Query Format

A query can be a map, or a vector of zero or more maps followed by zero or more keyword-value clauses. For example:

    [{:from :film} :select [:title :actor.name] :limit 1]

Any paths to related entities referenced outside of `:include` and `:with` will trigger outer joins when the query is executed.

Supported clauses:

         :from - name of the entity to query
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
         :join - explicit inner join; e.g., [[foo :f] [:= :id :f.id]]
    :left-join - explicit left outer join
      :options - a map with the following optional keys:
                   :join-type - whether to perform an :outer (the default) or
                                :inner join for fields selected from related
                                entities

Predicates are vectors of the form [op arg1 arg2 ...], where args are
paths, other predicates, etc. Built-in ops:

    :and :or and :xor
    := :not= :< :<= :> :>=
    :in :not-in :like :not-like :between
    :+ :- :* :/ :% :mod :| :& :^

Example predicate: `[:and [:= "Drama" :category.name] [:< 90 :length 180]]`

Aggregates are keywords that begin with % - e.g., `:%count.actor.id`

Bindable parameters are denoted with a leading ? - e.g., `:?actor-name`

### `query` Function

Arguments: `[ds q & opts]`

Executes a query against data source in a single round-trip.

The query can be one of the following:

* Query map/vector - see above for format
* PreparedQuery record - see `prepare-query`
* SQL string
* clojure.java.jdbc-style [sql params] vector

By default, returns a sequence of maps, with nested maps and sequences
for values selected from related entities. Example:

```clj 
(query ds {:from :film :select [:title :actor.name] :where [:= 1 :id]})
=> [{:title "Lawrence of Arabia"
     :actor [{:name "Peter O'Toole"} {:name "Omar Sharif"}]}]
```

NOTE: using the `:limit` clause may truncate nested values from to-many
relationships. To limit your query to a single top-level entity record while
retrieving all related records, restrict the results using the `:where` clause,
or use the `querym` function.

Keyword options:

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
                consistent)

### Other Query Functions


#### `querym [ds q & opts]`
  
Like `query` but may perform multiple data source round trips - one for
each selected second-level path segment that is part of a path that contains
a to-many relationship. Primary keys of the top-level entity results are used
to fetch the related results.

`:limit` and `:where` clauses apply only to the top-level entity query.

```clj
;; 3 database round trips - one for each to-many relationship
(c/querym ds [:from :film
              :include [:language :category :actor]
              :limit 10])
```

#### `querym1 [ds q & opts]`

Adds a "limit 1" clause to the query and executes it, potentially in
multiple round trips (one for each to-many relationship selected - the limit
clause will not affect these).

```clj
;; 3 database round trips, one result
(c/querym1 ds [:from :film
               :include [:language :category :actor]])
```

#### `query-count [ds q & opts]`

Returns the number of matching results for a query. By default, returns
the count of distinct top-level entity results. Set the `:flat` option to true
to return the count of ALL rows, including to-many rows with redundant
top-level values.

```clj
(c/query-count ds [:from :film :without :rental])
=> 226
```

#### `by-id [ds ename id & [q & opts]]`

Fetches the entity record from the data source whose primary key value is
equal to `id`. Uses `querym1` to execute the query, so multiple round trips
to the data source may occur. Query clauses from `q` will be merged into the
generated query.

#### `getf [results path]`

Returns one or more nested field values from the given query result or
results, traversing into related results as necessary, according to a
dotted keyword path. `path` and `results` can be swapped as arguments.
The following calls are equivalent:

    (getf :actor.name results)
    (getf results :actor.name)
  
If invoked with one argument, returns a partial function.

#### `getf1 [results path]`

Returns the same as `getf` except if the result would be a sequence, in
which case it returns the first element.

#### `queryf [ds q & opts]`

Same as `query`, but additionally calls `getf` using the first selected path.
Example:

```clj
(queryf ds {:from :film :select :actor.name :where [:= 1 :id]})
=> ["Peter O'Toole" "Omar Sharif"]
```

#### `queryf1 [ds q & opts]`

Same as `query`, but additionally calls getf1 using the first selected path.
Example:

```clj
(queryf1 ds {:from :film :select :actor.name :where [:= 1 :id]})
=> "Peter O'Toole"
```

#### `prepare-query [ds q & opts]`

Return a `PreparedQuery` record, which contains ready-to-execute SQL and
other necessary meta data. When executed, the query will accept bindable
parameters. (Bindable paramters can be included in queries using keywords
like `:?actor-name`.)

Unlike a JDBC PreparedStatement, a PreparedQuery record contains no
connection-specific information and can be reused at any time.

```clj
(let [pq (c/prepare-query
           ds [:from :film
               :where [:= :?country :renter.country-name]])]
  (c/query ds pq :params {:country "Canada"}))
```

#### `to-sql [ds-or-dm q & opts]`

Returns a clojure.java.jdbc-compatible [sql params] vector for the given
query.

```clj
(c/to-sql ds [:select :film.actor])
=> ["SELECT \"actor\".\"id\" AS \"actor.id\", \"actor\".\"name\" AS \"actor.name\" FROM \"PUBLIC\".\"film\" AS \"film\" LEFT JOIN \"PUBLIC\".\"film_actor\" AS \"film_actor\" ON \"film\".\"id\" = \"film_actor\".\"film_id\" LEFT JOIN \"PUBLIC\".\"actor\" AS \"actor\" ON \"film_actor\".\"actor_id\" = \"actor\".\"id\""]
```

### Manipulation Functions

* __`save!`__ `[ds ename values & opts]`
* __`insert!`__ `[ds ename map-or-maps & opts]`
* __`update!`__ `[ds ename values pred & opts]`
* __`delete!`__ `[ds ename pred]`
* __`delete-ids!`__ `[ds ename id-or-ids]`
* __`cascading-delete!`__ `[ds ename]`
* __`cascading-delete-ids!`__ `[ds ename id-or-ids]`
* __`execute!`__ `[ds q]`

### Debugging

```clj
;; Prints all SQL queries
(c/verbose
  (c/querym [:from :film :select [:id :actor]]))
  
;; Prints SQL, rolls back changes
(c/with-debug ds
  (c/cascading-delete-ids! film 1))
```

## License


Copyright © 2013 Justin Kramer

Distributed under the Eclipse Public License