(ns cantata.reflect
  "Tools for examining a the structural components of a data source"
  (:require [cantata.util :as cu]
            [clojure.java.jdbc :as jd]
            [clojure.string :as string])
  (:import [java.sql ResultSet ResultSetMetaData PreparedStatement
            Connection DatabaseMetaData]))

(set! *warn-on-reflection* true)

(defn identifier
  "Transforms a name from database lingo into a nice, lower-case Clojure
  keyword with dashes"
  [x & [prefix]]
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

(defn reflect-catalogs
  "Returns information about catalog in a data source"
  [ds]
  (jd/with-db-transaction
    [ds ds]
    (get-results
      (.getCatalogs (get-db-meta ds)))))

(defn reflect-tables
  "Returns information about tables in a data source"
  [ds]
  (jd/with-db-transaction
    [ds ds]
    (get-results
      (.getTables (get-db-meta ds) nil nil nil nil))))

(defn reflect-columns
  "Returns information about columns in a table and/or data source"
  ([ds]
    (reflect-columns ds nil))
  ([ds table]
    (jd/with-db-transaction
      [ds ds]
      (get-results
        (.getColumns (get-db-meta ds) nil nil table nil)))))

(defn reflect-foreign-keys
  "Returns information about foreign keys in a table and/or data source"
  ([ds]
    (reflect-foreign-keys ds nil))
  ([ds table]
    (jd/with-db-transaction
      [ds ds]
      (get-results
        (.getImportedKeys (get-db-meta ds) nil nil table)))))

(defn reflect-primary-keys
  "Returns information about primary keys in a table and/or data source"
  ([ds]
    (reflect-primary-keys ds nil))
  ([ds table]
    (jd/with-db-transaction
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
   :bytes ["BLOB" "TINYBLOB" "MEDIUMBLOB" "LONGBLOB" "BINARY" "VARBINARY"
         	 "LONGVARBINARY" "bytea"]})

(def db-type->type
  (into {} (for [[t dts] type->db-type
                 dt dts]
             [dt t])))

;;;;

(defn reflect-entities
  "Returns entity specs for all tables found in a data source"
  [ds & {:keys [table-prefix]}]
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

(defn reflect-fields
  "Returns field specs for all columns found in a data source table"
  [ds table & {:keys [column-prefix]}]
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

(defn reflect-rels
  "Returns rel specs based on foreign keys found in a data source table"
  [ds table & {:keys [column-prefix]}]
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

(defn reflect-pk
  "Returns the primary key name for a table found in a data source"
  [ds table & {:keys [column-prefix]}]
  (when cu/*verbose*
    (println "Reflecting for" table "PK"))
  (let [imetas (reflect-primary-keys ds table)
        pks (for [imeta imetas]
              (identifier (:column-name imeta) column-prefix))]
    (if (= 1 (count pks))
      (first pks)
      (vec pks))))
