(ns cantata.dsl
  "Functions for building queries piecemeal"
  (:require [cantata.query :as cq]
            [cantata.util :as cu]))

(defn from
  "Sets the entity name as the value of the :from clause"
  ([ename]
    [:from ename])
  ([q ename]
    (conj (cu/vecify q) :from ename)))

(defn select
  "Adds wildcards or paths of fields, relationships, or aggregates to
  the :select clause; unlike SQL, unqualified names will be assumed to refer
  to the :from entity.

  Aggregates are keywords that begin with % - e.g., `:%count.actor.id`"
  [q & fields]
  (conj (cu/vecify q) :select fields))

(defn replace-select
  "Replaces the :select clause"
  [q & fields]
  (conj (cu/vecify q) :replace-select fields))

(defn un-select
  "Removes paths from the :select clause"
  [q & fields]
  (conj (cu/vecify q) :un-select fields))

(defn ^:private get-pred [preds]
  (let [[logic-op preds] (if (cq/logic-ops (first preds))
                           [(first preds) (rest preds)]
                           [:and preds])
        pred1 (first preds)
        preds (if (map? pred1)
                (cons (into [logic-op] (for [[field val] pred1]
                                         [:= field val]))
                      (rest preds))
                preds)]
    (if (< 1 (count preds))
      (into [logic-op] preds)
      (first preds))))

(defn where
  "Adds predicates to the :where clause.

  If the argument after the query is one of :and, :or, :xor, or :not, uses
  that logical operation to combine multiple predicate arguments. Otherwise,
  defaults to :and.

  Predicates are vectors of the form [op arg1 arg2 ...], where args are
  paths, other predicates, etc. Built-in ops:

    :and :or :xor :not
    := :not= :< :<= :> :>=
    :in :not-in :like :not-like :between
    :+ :- :* :/ :% :mod :| :& :^

  This function accepts a special map argument that can be used as shorthand:

    (where {} {:category.name \"Drama\" :rating \"R\"})
    => [{} :where [:and [:= :category.name \"Drama\"] [:= :rating \"R\"]]]

  Other examples:

    (where {} [:= \"Drama\" :category.name] [:< 90 :length 180])
    => [{} :where [:and [:= \"Drama\" :category.name] [:< 90 :length 180]]]

    (where {} :or [:= \"Drama\" :category.name] [:< 90 :length 180])
    => [{} :where [:or [:= \"Drama\" :category.name] [:< 90 :length 180]]]"
  [q & preds]
  (conj (cu/vecify q) :where (get-pred preds)))

(defn replace-where
  "Replaces all predicates in the :where clause"
  [q & preds]
  (conj (cu/vecify q) :replace-where (get-pred preds)))

(defn having
  "Adds predicates to the :having clause. See `where` for predicate format."
  [q & preds]
  (conj (cu/vecify q) :having (get-pred preds)))

(defn replace-having
  "Replaces all predicates in the :having clause"
  [q & preds]
  (conj (cu/vecify q) :replace-having (get-pred preds)))

(defn order-by
  "Adds fields to the :order-by clause. Fields can be a path or vector like
  [path :asc] or [path :desc] to indicate ascending and descending sort order,
  respectively."
  [q & fields]
  (conj (cu/vecify q) :order-by fields))

(defn replace-order-by
  "Replaces all fields in the :order-by clause"
  [q & fields]
  (conj (cu/vecify q) :replace-order-by fields))

(defn un-order-by
  "Removes fields from the :order-by clause"
  [q & fields]
  (conj (cu/vecify q) :un-order-by fields))

(defn group
  "Adds fields to the :group-by clause"
  [q & fields]
  (conj (cu/vecify q) :group-by fields))

(defn replace-group
  "Replaces all fields in the :group-by clause"
  [q & fields]
  (conj (cu/vecify q) :replace-group-by fields))

(defn un-group
  "Removes fields from the :group-by clause"
  [q & fields]
  (conj (cu/vecify q) :un-group-by fields))

(defn modifiers
  "Adds keyword modifiers to the :modifiers clause. Valid modifiers:
    :distinct - return distinct results"
  [q & mods]
  (conj (cu/vecify q) :modifiers mods))

(defn replace-modifiers
  "Replaces keyword modifiers in the :modifiers clause"
  [q & mods]
  (conj (cu/vecify q) :replace-modifiers mods))

(defn un-modifiers
  "Removes keyword modifiers from the :modifiers clause"
  [q & mods]
  (conj (cu/vecify q) :un-modifiers mods))

(defn include
  "Adds to the :include clause. May be one or more relationship names to
  perform a left outer join with. May also be a map of the form:

    {:rel-name [:foo :bar :baz]}

  to select specific fields from related entities."
  [q & incls]
  (conj (cu/vecify q) :include incls))

(defn replace-include
  "Replaces an :include clause"
  [q & incls]
  (conj (cu/vecify q) :replace-include incls))

(defn un-include
  "Removes from the :include clause"
  [q & incls]
  (conj (cu/vecify q) :un-include incls))

(defn with
  "Adds to the :with clause. May be one or more relationship names to
  perform an inner join with. May also be a map of the form:

    {:rel-name [:foo :bar :baz]}

  to select specific fields from related entities."
  [q & withs]
  (conj (cu/vecify q) :with withs))

(defn replace-with
  "Replaces a :with clause"
  [q & withs]
  (conj (cu/vecify q) :replace-with withs))

(defn un-with
  "Removes from a :with clause"
  [q & withs]
  (conj (cu/vecify q) :un-with withs))

(defn without
  "Adds relationship names to the :without clause, which makes the query
  return results that have no related entity records for the relationship names"
  [q & rel-names]
  (conj (cu/vecify q) :without rel-names))

(defn replace-without
  "Replaces the :without clause"
  [q & rel-names]
  (conj (cu/vecify q) :replace-without rel-names))

(defn un-without
  "Removes relationship names from the :without clause"
  [q & rel-names]
  (conj (cu/vecify q) :un-without rel-names))

(defn options
  "Merges options into the :options clause"
  [q & {:as opts}]
  (conj (cu/vecify q) :options opts))

(defn replace-options
  "Replaces options in the :options clause. Valid options:
    :join-type - whether to perform an :outer (the default) or
                 :inner join for fields selected from related
                 entities"
  [q & {:as opts}]
  (conj (cu/vecify q) :replace-options opts))

(defn un-options
  "Removes options with the given key names from the :options clause"
  [q & opt-keys]
  (conj (cu/vecify q) :un-options opt-keys))

(defn join
  "Adds explicit inner joins -- i.e., the :join clause -- to the query.
  Expects pairs of \"to, on\" pairs. E.g.,

    (join [:foo :f] [:= :id :f.id]
          [:bar :b] [:= :id :b.id])"
  [q & [to on & more :as joins]]
  (conj (cu/vecify q) :join joins))

(defn replace-join
  "Replaces a :join clause"
  [q & [to on & more :as joins]]
  (conj (cu/vecify q) :replace-join joins))

(defn un-join
  "Removes from the :join clause"
  [q & [to on & more :as joins]]
  (conj (cu/vecify q) :un-join joins))

(defn left-join
  "Adds explicit left outer joins -- i.e., the :left-join clause -- to the
  query. Expects pairs of \"to, on\" pairs. E.g.,

    (join [:foo :f] [:= :id :f.id]
          [:bar :b] [:= :id :b.id])"
  [q & [to on & more :as joins]]
  (conj (cu/vecify q) :left-join joins))

(defn replace-left-join
  "Replaces a :left-join clause"
  [q & [to on & more :as joins]]
  (conj (cu/vecify q) :replace-left-join joins))

(defn un-left-join
  "Removes from a :left-join clause"
  [q & [to on & more :as joins]]
  (conj (cu/vecify q) :un-left-join joins))

(defn limit
  "Sets the :limit clause for a query, which limits the number of results
  returned"
  [q num]
  (conj (cu/vecify q) :limit num))

(defn offset
  "Sets the :offset clause for a query, which determines the offset into
  the result set when the query is executed"
  [q num]
  (conj (cu/vecify q) :offset num))

