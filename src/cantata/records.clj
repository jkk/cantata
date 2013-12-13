(ns cantata.records
  "Records used in Cantata implementation")

(defrecord Field [name type db-name db-type])
(defrecord Rel [name ename key other-key reverse one])
(defrecord Shortcut [name path])
(defrecord Entity [name pk fields rels db-name db-schema shortcuts hooks])
(defrecord ValidationProblem [keys msg])
(defrecord DataModel [name entities])
(defrecord Resolved [type value])
(defrecord ChainLink [from to from-path to-path rel])
(defrecord ResolvedPath [final-path root chain resolved shortcuts])
(defrecord AggOp [op path resolved-path])
(defrecord PreparedQuery [expanded-query env sql param-names param-values added-paths])

(defrecord Elided [])

(defmethod print-method Elided [^Elided o ^java.io.Writer w]
  (.write w "#<Elided>"))

(defmethod print-dup Elided [o w]
   (print-method o w))

(def elided (Elided.))

(defmethod print-method PreparedQuery [o ^java.io.Writer w]
  (.write w (pr-str (assoc (into {} o)
                           :env elided))))