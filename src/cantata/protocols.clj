(ns cantata.protocols
  (:import org.joda.time.DateTimeZone
           org.joda.time.LocalDate
           org.joda.time.LocalTime
           org.joda.time.DateTime))

(set! *warn-on-reflection* true)

;; Implemented as protocols for performance

(defprotocol ClobToStr
  (clob->str [x]))

(extend-protocol ClobToStr
  java.sql.Clob (clob->str [x] (slurp (.getCharacterStream x)))
  Object (clob->str [x] x)
  nil (clob->str [_] nil))

(defprotocol DateToJoda
  (date->joda [x]))

(extend-protocol DateToJoda
  java.sql.Date
  (date->joda [x]
    (LocalDate. (.getTime ^java.sql.Date x) DateTimeZone/UTC))
  java.sql.Time
  (date-joda [x]
    (LocalTime. (.getTime ^java.sql.Time x) DateTimeZone/UTC))
  java.sql.Timestamp
  (date->joda [x]
    (DateTime. (.getTime ^java.sql.Timestamp x) DateTimeZone/UTC))
  Object (date->joda [x] x)
  nil (date->joda [_] nil))

(defprotocol JodaToDate
  (joda->date [x]))

(extend-protocol JodaToDate
  LocalDate
  (joda->date [x]
    (java.sql.Date. (.getTime ^java.util.Date (.toDate ^LocalDate x))))
  LocalTime
  (joda->date [x]
    (java.sql.Time. (.getMillis ^DateTime (.toDateTimeToday ^LocalTime x))))
  DateTime
  (joda->date [x]
    (java.sql.Timestamp. (.getMillis ^DateTime x)))
  Object (joda->date [x] x)
  nil (joda->date [_] nil))

