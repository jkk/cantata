(ns cantata.reflect
  (:require [cantata.util :as cu]
            [clojure.java.jdbc :as jd]
            [clojure.string :as string])
  (:import [java.sql ResultSet ResultSetMetaData PreparedStatement
            Connection DatabaseMetaData]))

(defn guess-db-name [ename]
  (string/replace (name ename) "-" "_"))

(defn identifier [x]
  (-> x (string/replace #"[_\s]" "-") string/lower-case keyword))

(defn reflect-entities [ds]
  (when cu/*verbose*
    (println "Reflecting for entities"))
  (with-open [conn ^Connection (jd/get-connection ds)]
    (let [dbmeta ^DatabaseMetaData (.getMetaData conn)
          tmetas (doall
                   (jd/result-set-seq
                     (.getTables dbmeta nil nil nil nil)))]
      (doall
        (for [tmeta tmetas
              :when (= "TABLE" (:table_type tmeta))]
          (let [tname (:table_name tmeta)
                ename (identifier tname)]
            ;; TODO: schema
            {:name ename
             :db-name tname}))))))

(defn reflect-fields [ds table]
  (when cu/*verbose*
    (println "Reflecting for" table "fields"))
  (with-open [conn ^Connection (jd/get-connection ds)]
    (let [dbmeta ^DatabaseMetaData (.getMetaData conn)
          cmetas (doall
                   (jd/result-set-seq
                     (.getColumns dbmeta nil nil table nil)))]
      (doall
        (for [cmeta cmetas]
          (let [cname (:column_name cmeta)
                field-name (identifier cname)]
            ;; TODO: type
            {:name field-name
             :db-name cname}))))))

(defn reflect-rels [ds table]
  (when cu/*verbose*
    (println "Reflecting for" table "rels"))
  (with-open [conn ^Connection (jd/get-connection ds)]
    (let [dbmeta ^DatabaseMetaData (.getMetaData conn)
          imetas (doall
                   (jd/result-set-seq
                     (.getImportedKeys dbmeta nil nil table)))]
      (doall
        (for [imeta imetas]
          (let [key (identifier (:fkcolumn_name imeta))]
            ;; TODO: more robust name generation
            {:name (keyword (string/replace (name key) #"-id$" ""))
             :ename (identifier (:pktable_name imeta))
             :key (identifier (:fkcolumn_name imeta))
             :other-key (identifier (:pkcolumn_name imeta))}))))))

(defn reflect-pk [ds table]
  (when cu/*verbose*
    (println "Reflecting for" table "PK"))
  (with-open [conn ^Connection (jd/get-connection ds)]
    (let [dbmeta ^DatabaseMetaData (.getMetaData conn)
          imetas (doall
                   (jd/result-set-seq
                     (.getPrimaryKeys dbmeta nil nil table)))
          pks (for [imeta imetas]
                (identifier (:column_name imeta)))]
      (if (= 1 (count pks))
        (first pks)
        (vec pks)))))
