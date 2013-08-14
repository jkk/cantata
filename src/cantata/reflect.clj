(ns cantata.reflect
  (:require [cantata.util :as cu]
            [clojure.java.jdbc :as jd]
            [clojure.string :as string])
  (:import [java.sql ResultSet ResultSetMetaData PreparedStatement
            Connection DatabaseMetaData]))

(set! *warn-on-reflection* true)

(defn guess-db-name [ename]
  (string/replace (name ename) "-" "_"))

(defn identifier [x]
  (-> x (string/replace #"[_\s]" "-") string/lower-case keyword))

(defn ^:private dasherize [s]
  (-> s
    (string/lower-case)
    (string/replace "_" "-")))

(defn ^:private get-results [rs]
  (vec (jd/result-set-seq rs :identifiers dasherize)))

(defn ^:private ^DatabaseMetaData get-db-meta [ds]
  (.getMetaData ^Connection (:connection ds)))

(defn reflect-tables [ds]
  (jd/db-transaction
    [ds ds]
    (get-results
      (.getTables (get-db-meta ds) nil nil nil nil))))

(defn reflect-columns
  ([ds]
    (reflect-columns ds nil))
  ([ds table]
    (jd/db-transaction
      [ds ds]
      (get-results
        (.getColumns (get-db-meta ds) nil nil table nil)))))

(defn reflect-foreign-keys
  ([ds]
    (reflect-foreign-keys ds nil))
  ([ds table]
    (jd/db-transaction
      [ds ds]
      (get-results
        (.getImportedKeys (get-db-meta ds) nil nil table)))))

(defn reflect-primary-keys
  ([ds]
    (reflect-primary-keys ds nil))
  ([ds table]
    (jd/db-transaction
      [ds ds]
      (get-results
        (.getPrimaryKeys (get-db-meta ds) nil nil table)))))

;;;;

(defn reflect-entities [ds]
  (when cu/*verbose*
    (println "Reflecting for entities"))
  (doall
    (for [tmeta (reflect-tables ds)
          :when (= "TABLE" (:table-type tmeta))]
      (let [tname (:table-name tmeta)
            ename (identifier tname)]
        {:name ename
         :db-schema (:table-schem tmeta)
         :db-name tname}))))

(defn reflect-fields [ds table]
  (when cu/*verbose*
    (println "Reflecting for" table "fields"))
  (doall
    (for [cmeta (reflect-columns ds table)]
      (let [cname (:column-name cmeta)
            field-name (identifier cname)]
        ;; TODO: type
        {:name field-name
         :db-name cname}))))

(defn reflect-rels [ds table]
  (when cu/*verbose*
    (println "Reflecting for" table "rels"))
  (doall
    (for [imeta (reflect-foreign-keys ds table)]
      (let [key (identifier (:fkcolumn-name imeta))]
        ;; TODO: more robust name generation
        {:name (keyword (string/replace (name key) #"-id$" ""))
         :ename (identifier (:pktable-name imeta))
         :key (identifier (:fkcolumn-name imeta))
         :other-key (identifier (:pkcolumn-name imeta))}))))

(defn reflect-pk [ds table]
  (when cu/*verbose*
    (println "Reflecting for" table "PK"))
  (let [imetas (reflect-primary-keys ds table)
        pks (for [imeta imetas]
              (identifier (:column-name imeta)))]
    (if (= 1 (count pks))
      (first pks)
      (vec pks))))
