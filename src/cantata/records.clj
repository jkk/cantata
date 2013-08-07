(ns cantata.records)

(defrecord Field [name type db-name db-type])
(defrecord Rel [name ename key other-key reverse])
(defrecord Shortcut [name path])
(defrecord Entity [name pk fields rels db-name db-schema shortcuts])
(defrecord DataModel [entities])
(defrecord Resolved [type value])
(defrecord ChainLink [from to from-path to-path rel])
(defrecord ResolvedPath [root chain resolved shortcuts])

