(ns cantata.query.qualify
  "Turn a Cantata-style query into a Honey SQL-compatible one"
  (:require [cantata.util :as cu]
            [cantata.query.util :as qu]
            [honeysql.core :as hq]))

(defn identifier
  "Turns an optionally-qualified string or keyword into a quoted identifier"
  ([x quoting]
    (hq/raw (hq/quote-identifier x :style quoting :split false)))
  ([x y quoting]
    (if-not x
      (identifier y quoting)
      (hq/raw (str (hq/quote-identifier x :style quoting :split false)
                   "."
                   (hq/quote-identifier y :style quoting :split false))))))

(defmulti qualify
  "Transforms a ResolvedPath or other object into a fully-qualified, quoted
  identifier or other SQL value, taking into account entity and field name
  mappings"
  (fn [x quoting]
    (-> x :resolved :type)))

(defmethod qualify :default [x quoting]
  x)

(defmethod qualify :joined-entity [x quoting]
  (let [ent (-> x :resolved :value)]
    (identifier (:db-schema ent) (:db-name ent) quoting)))

(defmethod qualify :entity [x quoting]
  (let [ent (-> x :resolved :value)]
    (identifier (:db-schema ent) (:db-name ent) quoting)))

(defmethod qualify :field [x quoting]
  (let [fname (-> x :resolved :value :db-name)
        chain (:chain x)]
    (if (seq chain)
      (identifier (-> chain peek :to-path) fname quoting)
      (identifier (-> x :root :name) fname quoting))))

(defmethod qualify :joined-field [x quoting]
  (if-let [fname (-> x :resolved :value :db-name)]
    (identifier (-> x :final-path cu/unqualify first) fname quoting)
    (let [[qual basename] (cu/unqualify (:final-path x))]
      (identifier qual basename quoting))))

(defmethod qualify :agg-op [x quoting]
  (let [v (-> x :resolved :value)
        fname (-> v :resolved-path :value :db-name)
        chain (:chain x)]
    (hq/call (:op v)
             (if (= :* (:path v))
               (hq/raw "*")
               (if (seq chain)
                 (if (qu/wildcard? (:path v))
                   (hq/raw (str (hq/quote-identifier (-> chain peek :to-path)
                                                     :style quoting :split false)
                                ".*"))
                   (identifier (-> chain peek :to-path) fname quoting))
                 (identifier (-> x :root :name) fname quoting))))))

(defmethod qualify :param [x quoting]
  (-> x :resolved :value))

(defmulti qualify-clause
  "Qualifies and quotes all identifiers in a query clause, taking into account
  entity and field name mappings"
  (fn [clause clause-val quoting env]
    clause))

(defmethod qualify-clause :default [_ cval _ _]
  cval)

(defmethod qualify-clause :from [_ from quoting env]
  (if-let [ent (-> (env from) :resolved :value)]
    [[(identifier (:db-schema ent) (:db-name ent) quoting) from]]
    [from]))

(declare qualify-query)

(defn ^:private get-subquery-env [subq]
  (:cantata/env (meta subq)))

(defmethod qualify-clause :select [_ select quoting env]
  (for [path select]
    (let [[field alias] (if (vector? path)
                          path
                          [path])
          qfield (if (map? field)
                   (qualify-query field quoting (or (get-subquery-env field) env))
                   (qualify (env field field) quoting))]
      (if (or (= :* field) (qu/wildcard? field))
        qfield
        (if (or (qu/raw? field) (qu/call? field)
                (number? field) (string? field))
          path
          [qfield (or alias (identifier (or (:final-path (env field)) field)
                                        quoting))])))))

(defn qualify-pred-fields [pred quoting env]
  (when pred
    (let [fields (qu/get-predicate-fields pred)
          smap (into {} (for [f fields]
                          [f (if (map? f)
                               (qualify-query f quoting (or (get-subquery-env f) env))
                               (qualify (env f f) quoting))]) )]
      (qu/replace-predicate-fields pred smap))))

(defmethod qualify-clause :where [_ where quoting env]
 (qualify-pred-fields where quoting env))

(defmethod qualify-clause :having [_ where quoting env]
 (qualify-pred-fields where quoting env))

(defmethod qualify-clause :order-by [_ order-by quoting env]
 (when order-by
   (for [f order-by]
     (let [fname (if (coll? f) (first f) f)
           dir (when (coll? f) (second f))
           qfield (qualify (env fname fname) quoting)]
       (if dir
         [qfield dir]
         qfield)))))

(defmethod qualify-clause :group-by [_ group-by quoting env]
 (when group-by
   (for [fname group-by]
     (qualify (env fname fname) quoting))))

;; TODO: subqueries
(defn ^:private qualify-join [joins quoting env]
 (mapcat (fn [[to on]]
           (let [ename (if (vector? to) (first to) to)
                 path (if (vector? to)
                        (second to)
                        ename)
                 qpath (identifier path quoting)
                 qename (if (map? ename)
                          (qualify-query ename quoting (or (get-subquery-env ename) env))
                          (qualify (env path path) quoting))
                 on* (qualify-pred-fields on quoting env)]
             [[qename qpath] on*]))
         (partition 2 joins)))

(defmethod qualify-clause :join [_ joins quoting env]
 (qualify-join joins quoting env))

(defmethod qualify-clause :left-join [_ joins quoting env]
 (qualify-join joins quoting env))

(defmethod qualify-clause :right-join [_ joins quoting env]
 (qualify-join joins quoting env))

;; TODO: traverse into SqlCalls?
(defn qualify-query
  "Qualifies and quotes all identifiers in a query, taking into account entity
  and field name mappings"
  ([q]
    (qualify-query q nil))
  ([q quoting]
    (qualify-query q quoting {}))
  ([q quoting env]
    (reduce-kv
      (fn [q clause cval]
        (let [cval* (qualify-clause clause cval quoting env)]
          (if (and (not (nil? cval))
                   (or (not (coll? cval*))
                       (seq cval*)))
            (assoc q clause cval*)
            q)))
      q q)))