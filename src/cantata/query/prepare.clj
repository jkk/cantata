(ns cantata.query.prepare
  "Prepare a query for execution by expanding and qualifying it"
  (:require [cantata.query.expand :as qe]
            [cantata.query.qualify :as qq]
            [cantata.query.util :as qu]
            [cantata.records :as r]
            [cantata.data-model :as dm]
            [cantata.util :refer [throw-info]]
            [honeysql.core :as hq]))

(defn to-sql
  "Transforms a Cantata query into a clojure.java.jdbc-compatible [sql params]
  vector, leveraging the provided data model and other information"
  [q & {:keys [data-model quoting expanded env params]}]
  (cond
   (string? q) [q]
   (and (vector? q) (string? (first q))) q
   (and (sequential? q) (string? (first q))) (vec q)
   :else (let [[eq env] (if (or expanded (not data-model))
                          [q env]
                          (qe/expand-query data-model q))
               qq (qq/qualify-query eq quoting (or env {}))]
           (hq/format qq
                      :quoting quoting
                      :params params))))

;; TODO: prepared statements; need a way to manage open/close scope;
;; maybe piggyback on transaction scope?
(defn prepare-query
  "See cantata.core/prepare-query"
  [dm q & {:keys [quoting expanded env force-pk] :as opts}]
  (let [[eq env added-paths] (if (or expanded (qu/plain-sql? q))
                               [q env]
                               (qe/expand-query dm q :force-pk (not (false? force-pk))))
        [sql & params] (apply to-sql eq
                              :data-model dm
                              :quoting quoting
                              :expanded true
                              :env env
                              (apply concat (dissoc opts :params)))
        param-counter (atom 0) ;for naming anonymous params
        [param-names param-values]
        (loop [ret []
               params params
               named-params (map qu/param-name
                                 (filter qu/param? (keys env)))
               param-values {}]
          (if (empty? params)
            [ret param-values]
            (let [param (first params)]
              (if (nil? param)
                (recur (conj ret (first named-params))
                       (rest params)
                       (rest named-params)
                       param-values)
                (let [param-name (keyword (str "_" (swap! param-counter inc)))]
                  (recur (conj ret param-name)
                         (rest params)
                         named-params
                         (assoc param-values param-name param)))))))]
    (r/->PreparedQuery eq env sql param-names param-values added-paths)))

(defn prepare-count-query
  "Transformed a prepared query into a query that returns the count of rows
  that would match"
  [dm prepared-q & {:keys [flat quoting] :as opts}]
  (when-not (qu/prepared? prepared-q)
    (throw-info "Query must be prepared" {:q prepared-q}))
  (when-not (:env prepared-q)
    (throw-info "Query count transformation not supported on plain SQL"
                {:q prepared-q}))
  (let [eq (:expanded-query prepared-q)
        ent (dm/entity dm (:from eq))
        select (if (or flat
                       (not-any? eq qu/join-clauses))
                 [[(hq/call :count (hq/raw "*")) :cnt]]
                 (let [npk (dm/normalize-pk (:pk ent))
                       qpk (for [pk npk]
                             (qq/qualify
                               (dm/resolve-path dm ent npk)
                               quoting))]
                   [[(apply hq/call :count-distinct qpk) :cnt]]))
        eq (-> eq
             (dissoc :limit :offset)
             (assoc :select select))]
    (assoc prepared-q
           :expanded-query eq
           :sql (first
                  (apply to-sql eq
                         :data-model dm
                         :quoting quoting
                         :expanded true
                         :env (:env prepared-q)
                         (apply concat (dissoc opts :params)))))))