(ns cantata.reflect
  (:require [cantata.util :as cu]
            [clojure.java.jdbc :as jd]
            [clojure.string :as string])
  (:import [java.sql ResultSet ResultSetMetaData PreparedStatement
            Connection DatabaseMetaData]))

(set! *warn-on-reflection* true)

(defn guess-db-name [ename]
  (string/replace (name ename) "-" "_"))

(defn identifier [x & [prefix]]
  (let [x (if prefix
            (string/replace-first x prefix "")
            x)]
    (-> x (string/replace #"[_\s]" "-") string/lower-case keyword)))

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

(def type->db-type
  {:int ["INTEGER" "INT" "SMALLINT" "TINYINT" "MEDIUMINT" "BIGINT" "YEAR"
         "integer" "int" "bigint" "int8" "int4" "int2" "bigserial" "serial8"
         "smallint" "serial" "serial4"]
   :double ["FLOAT" "DOUBLE" "REAL" "DOUBLE PRECISION"
            "double precision" "float8" "real" "float4"]
   :decimal ["DECIMAL" "NUMERIC" "DEC" "FIXED"
             "numeric" "decimal"]
   :boolean ["BOOLEAN" "BOOL" "BIT" "boolean" "bool" "bit"]
   :str ["VARCHAR" "CHAR" "CLOB" "ENUM" "SET" "TEXT" "TINYTEXT" "MEDIUMTEXT"
         "LONGTEXT" "NCHAR" "NCLOB" "LONGVARCHAR" "LONGNVARCHAR"
         "character varying" "varchar" "char" "character" "text"]
   :date ["DATE" "date"]
   :time ["TIME" "time" "timetz"]
   :datetime ["TIMESTAMP" "DATETIME" "timestamp" "timestamptz"]
   :blob ["BLOB" "TINYBLOB" "MEDIUMBLOB" "LONGBLOB" "BINARY" "VARBINARY"
          "LONGVARBINARY" "bytea"]})

(def db-type->type
  (into {} (for [[t dts] type->db-type
                 dt dts]
             [dt t])))

;;;;

(defn reflect-entities [ds & {:keys [table-prefix]}]
  (when cu/*verbose*
    (println "Reflecting for entities"))
  (doall
    (for [tmeta (reflect-tables ds)
          :when (= "TABLE" (:table-type tmeta))]
      (let [tname (:table-name tmeta)
            ename (identifier tname table-prefix)]
        {:name ename
         :db-schema (:table-schem tmeta)
         :db-name tname}))))

(defn reflect-fields [ds table & {:keys [column-prefix]}]
  (when cu/*verbose*
    (println "Reflecting for" table "fields"))
  (doall
    (for [cmeta (reflect-columns ds table)]
      (let [cname (:column-name cmeta)
            field-name (identifier cname column-prefix)
            db-type (:type-name cmeta)]
        ;; TODO: size?
        {:name field-name
         :db-name cname
         :db-type db-type
         :type (db-type->type db-type)}))))

(defn reflect-rels [ds table & {:keys [column-prefix]}]
  (when cu/*verbose*
    (println "Reflecting for" table "rels"))
  (doall
    (for [imeta (reflect-foreign-keys ds table)]
      (let [key (identifier (:fkcolumn-name imeta) column-prefix)]
        ;; TODO: more robust name generation
        {:name (identifier (string/replace (name key) #"-id$" "") column-prefix)
         :ename (identifier (:pktable-name imeta) column-prefix)
         :key (identifier (:fkcolumn-name imeta) column-prefix)
         :other-key (identifier (:pkcolumn-name imeta) column-prefix)}))))

(defn reflect-pk [ds table & {:keys [column-prefix]}]
  (when cu/*verbose*
    (println "Reflecting for" table "PK"))
  (let [imetas (reflect-primary-keys ds table)
        pks (for [imeta imetas]
              (identifier (:column-name imeta) column-prefix))]
    (if (= 1 (count pks))
      (first pks)
      (vec pks))))
