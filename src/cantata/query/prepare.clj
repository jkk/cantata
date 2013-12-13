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
  [q & {:keys [data-model quoting expanded env params return-param-names]}]
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
                      :params params
                      :return-param-names return-param-names))))

(defn prepare-query
  "See cantata.core/prepare-query"
  [dm q & {:keys [quoting expanded env force-pk] :as opts}]
  (let [[eq env added-paths] (if (or expanded (qu/plain-sql? q))
                               [q env]
                               (qe/expand-query dm q :force-pk (not (false? force-pk))))
        [sql params param-names] (cond
                                   (string? eq) [eq]
                                   (qu/plain-sql? eq) [(first eq) (rest eq)
                                                       (map #(keyword (str "_" %))
                                                            (range 1 (inc (count (rest eq)))))]
                                   :else (apply to-sql eq
                                                :data-model dm
                                                :quoting quoting
                                                :expanded true
                                                :env env
                                                :return-param-names true
                                                (apply concat (dissoc opts :params))))
        param-values (zipmap param-names params)]
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