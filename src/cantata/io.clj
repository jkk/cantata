(ns cantata.io
  (:require [cantata.data-source :as cds]
            [cantata.data-model :as cdm]
            [cantata.util :as cu :refer [throw-info]]
            [cantata.core :as c]
            [cantata.protocols :as cp]
            [clojure.java.io :as io]
            [clojure.edn :as edn]))

(defn export-data!
  "Exports entity data to out - a filename, File, etc. - in EDN format."
  [ds out & {:keys [enames]}]
  (with-open [w (io/writer out)]
    (binding [*out* w]
      (let [dm (cds/get-data-model ds)
            sorted-names (cdm/sort-entities dm)
            enames (if enames
                     (filter (set enames) sorted-names)
                     sorted-names)]
        (doseq [ename enames]
          (when cu/*verbose*
            (println "Exporting" ename))
          (c/with-query-rows cols rows ds [:from ename]
            (prn {:ename ename :cols cols})
            (doseq [row rows]
              (prn (mapv cp/joda->date row)))))))))

(defn import-data!
  "Imports entity data from in - a filename, File, etc."
  [ds in]
  (with-open [r (io/reader in)]
    (let [dm (cds/get-data-model ds)]
      (loop [ent nil
             cols nil
             lines (line-seq r)]
        (when (seq lines)
          (let [line (edn/read-string (first lines))]
            (if (map? line)
              (let [{:keys [ename cols]} line
                    ent (cdm/entity dm ename)]
                (when-not ent
                  (throw-info ["Unrecognized entity" ename]
                              {:line line :cols cols :ename ename}))
                (when cu/*verbose*
                  (println "Importing" ename))
                (recur ent cols (rest lines)))
              (do
                (when-not (and ent cols (sequential? line))
                  (throw-info "Invalid data file format"
                              {:line line :cols cols :ent ent}))
                (c/save! ds (:name ent) (zipmap cols line))
                (recur ent cols (rest lines))))))))))