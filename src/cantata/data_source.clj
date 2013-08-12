(ns cantata.data-source
  (:require [cantata.util :refer [throw-info]]))

(defn normalize-db-spec [db-spec]
  (cond
    (map? db-spec) db-spec
    (string? db-spec) {:connection-uri db-spec}
    (instance? java.net.URI db-spec) (normalize-db-spec (str db-spec))
    :else (throw-info "Unrecognized db-spec format" {:db-spec db-spec})))

;; TODO: configurable transformations: joda dates, clob/str, blank to nil