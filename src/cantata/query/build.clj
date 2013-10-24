(ns cantata.query.build
  "Build up queries and parts of queries"
  (:require [cantata.util :as cu]
            [cantata.query.util :as qu]
            [clojure.string :as string]))

(def ^:private clause-variants-re #"^(replace|un)\-")

(defmulti normalize-clause
  "Given a [clause-name clause-value] vector, normalizes the clause value as
  appropriate. E.g., wraps a non-sequential :select into a collection so
  that it can be operated on generically.

  Dispatches on clause-name."
  (fn [[clause-name clause-val]]
    (let [s (name clause-name)]
      (if (re-find clause-variants-re s)
        (keyword (string/replace s clause-variants-re ""))
        clause-name))))

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
      (zipmap clause-val (repeat {}))
      {clause-val {}})))

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
  (let [s (name clause)
        clause (if (re-find clause-variants-re s)
                 (keyword (string/replace s clause-variants-re ""))
                 clause)]
    (assoc q clause clause-val)))

(defmethod merge-clause :select [q [_ clause-val]]
  (update-in q [:select] concat clause-val))

(defmethod merge-clause :un-select [q [_ clause-val]]
  (update-in q [:select] #(remove (set clause-val) %)))

(defmethod merge-clause :where [q [_ clause-val]]
  (qu/merge-where q clause-val))

(defmethod merge-clause :having [q [_ clause-val]]
  (qu/merge-where q clause-val :having))

(defmethod merge-clause :include [q [_ clause-val]]
  (update-in q [:include] merge clause-val))

(defmethod merge-clause :un-include [q [_ clause-val]]
  (update-in q [:include] #(apply dissoc % (keys clause-val))))

(defmethod merge-clause :with [q [_ clause-val]]
  (update-in q [:with] merge clause-val))

(defmethod merge-clause :un-with [q [_ clause-val]]
  (update-in q [:with] #(apply dissoc % (keys clause-val))))

(defmethod merge-clause :without [q [_ clause-val]]
  (update-in q [:without] concat clause-val))

(defmethod merge-clause :un-without [q [_ clause-val]]
  (update-in q [:without] #(remove (set clause-val) %)))

(defmethod merge-clause :join [q [_ clause-val]]
  (update-in q [:join] concat clause-val))

(defn ^:private un-join [q clause unjoins]
  (update-in q [clause]
             (fn [joins]
               (apply concat (remove (set (partition 2 unjoins))
                                     (partition 2 joins))))))

(defmethod merge-clause :un-join [q [_ clause-val]]
  (un-join q :join clause-val))

(defmethod merge-clause :left-join [q [_ clause-val]]
  (update-in q [:left-join] concat clause-val))

(defmethod merge-clause :un-left-join [q [_ clause-val]]
  (un-join q :left-join clause-val))

(defmethod merge-clause :right-join [q [_ clause-val]]
  (update-in q [:right-join] concat clause-val))

(defmethod merge-clause :un-right-join [q [_ clause-val]]
  (un-join q :right-join clause-val))

(defmethod merge-clause :options [q [_ clause-val]]
  (update-in q [:options] merge clause-val))

(defmethod merge-clause :un-options [q [_ clause-val]]
  (update-in q [:options] #(apply dissoc % (cu/seqify clause-val))))

(defmethod merge-clause :order-by [q [_ clause-val]]
  (update-in q [:order-by] concat clause-val))

(defmethod merge-clause :un-order-by [q [_ clause-val]]
  (update-in q [:order-by] #(remove (set clause-val) %)))

(defmethod merge-clause :group-by [q [_ clause-val]]
  (update-in q [:group-by] concat clause-val))

(defmethod merge-clause :un-group-by [q [_ clause-val]]
  (update-in q [:group-by] #(remove (set clause-val) %)))

(defmethod merge-clause :modifiers [q [_ clause-val]]
  (update-in q [:modifiers] concat clause-val))

(defmethod merge-clause :un-modifiers [q [_ clause-val]]
  (update-in q [:modifiers] #(remove (set clause-val) %)))

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