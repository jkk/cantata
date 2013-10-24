(ns cantata.io
  "Get data into and out of Cantata"
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

(defn ^:private import-batches [ent cols lines batch-size save-fn]
  (loop [batch []
         lines (rest lines)]
    (let [line (edn/read-string (first lines))]
      (cond
        ;; finished with this entity
        (or (not line) (map? line))
        (do
          (save-fn ent batch)
          lines)
        ;; finished batch
        (= batch-size (count batch))
        (do
          (save-fn ent batch)
          (recur [] lines))
        ;; keep collecting lines for this batch
        (vector? line)
        (recur (conj batch (zipmap cols line)) (rest lines))
        ;; bad data
        :else
        (throw-info "Invalid row"
                    {:line line :cols cols :ename (:name ent)})))))

(defn import-data!
  "Imports entity data from in - a filename, File, etc. Set :update to true to
  check for presence of each record and, if present, update instead of insert."
  [ds in & {:keys [update batch-size] :or {batch-size 100}}]
  (let [save-fn (if update
                  (fn [ent ms] (doseq [m ms] (c/save! ds (:name ent) m)))
                  (fn [ent ms] (c/insert! ds (:name ent) ms :return-keys false)))
        dm (cds/get-data-model ds)]
    (with-open [r (io/reader in)]
      (loop [lines (line-seq r)]
        (when-let [line (edn/read-string (first lines))]
          (let [{:keys [ename cols]} line
                ent (cdm/entity dm ename)]
            (when-not ent
              (throw-info ["Unrecognized entity" ename]
                          {:line line :cols cols :ename ename}))
            (when cu/*verbose*
              (println "Importing" ename))
            (when-let [lines* (seq (import-batches ent cols lines batch-size save-fn))]
              (recur lines*))))))))