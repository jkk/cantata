(ns cantata.data-source)

(defprotocol IDataSource
  (query [this q callback])
  (query1 [this q callback])
  (query-count [this q callback])
  (save [this changes opts])
  (delete [this entity-name pred opts])
  (reflect-entities [this])
  (reflect-fields [this entity-name])
  (reflect-rels [this entity-name])
  (reflect-pk [this entity-name]))
