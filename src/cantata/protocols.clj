(ns cantata.protocols
  (:require [clojure.instant :as instant])
  (:import org.joda.time.DateTimeZone
           org.joda.time.LocalDate
           org.joda.time.LocalTime
           org.joda.time.DateTime))

(set! *warn-on-reflection* true)

;; For entity construction

(defprotocol ToInt
  (to-int [x]))

(extend-protocol ToInt
  Number
  (to-int [x] (long x))
  String
  (to-int [x] (Long/valueOf x))
  Character
  (to-int [x] (Character/getNumericValue x))
  java.util.Date
  (to-int [x] (.getTime x))
  DateTime
  (to-int [x] (.getMillis x))
  LocalDate
  (to-int [x] (.getTime ^java.util.Date (.toDate x)))
  nil
  (to-int [_] nil))

(defprotocol ToStr
  (to-str [x]))

(extend-protocol ToStr
  String
  (to-str [x] x)
  java.sql.Clob
  (to-str [x] (slurp (.getCharacterStream x)))
  java.sql.Blob
  (to-str [x] (to-str (.getBytes x 0 (.length x))))
  Character
  (to-str [x] (str x))
  Object
  (to-str [x] (pr-str x)) ;NOT using str here
  nil
  (to-str [_] nil))

(extend (Class/forName "[B") ;byte arrays
  ToStr
  {:to-str (fn [^"[B" x] (String. x "UTF-8"))})

(defprotocol ToDouble
  (to-double [x]))

(extend-protocol ToDouble
  Number
  (to-double [x] (double x))
  String
  (to-double [x] (Double/valueOf x))
  Character
  (to-double [x] (to-double (Character/getNumericValue x)))
  nil
  (to-double [_] nil))

(defprotocol ToDecimal
  (to-decimal [x]))

(extend-protocol ToDecimal
  Number
  (to-decimal [x] (bigdec x))
  String
  (to-decimal [x] (bigdec x))
  nil
  (to-double [_] nil))

(defprotocol ToBoolean
  (to-boolean [x]))

(extend-protocol ToBoolean
  Number
  (to-boolean [x] (not (zero? x)))
  String
  (to-boolean [x] (Boolean/valueOf x))
  nil
  (to-boolean [_] nil))

(defprotocol ToBytes
  (to-bytes [x]))

(extend-protocol ToBytes
  String
  (to-bytes [x] (.getBytes x "UTF-8"))
  java.sql.Blob
  (to-bytes [x] (.getBytes x 0 (.length x)))
  nil
  (to-bytes [_] nil))

(defprotocol ToDate
  (to-date [x]))

(def date-sdf (java.text.SimpleDateFormat. "yyyy-MM-dd"))

(extend-protocol ToDate
  java.util.Date
  (to-date [x] x)
  String
  (to-date [x] (.parse ^java.text.SimpleDateFormat date-sdf x))
  Number
  (to-date [x] (java.util.Date. (long x)))
  DateTime
  (to-date [x] (.toDate x))
  LocalDate
  (to-date [x] (.toDate x)))

(defprotocol ToTime
  (to-time [x]))

(def time-sdf (java.text.SimpleDateFormat. "HH:mm:ss"))

(extend-protocol ToTime
  java.util.Date
  (to-time [x] x)
  String
  (to-time [x] (.parse ^java.text.SimpleDateFormat time-sdf x))
  Number
  (to-time [x] (java.util.Date. (long x)))
  DateTime
  (to-time [x] (.toDate x))
  LocalTime
  (to-time [x] (.toDate ^DateTime (.toDateTimeToday x))))

(defprotocol ToDatetime
  (to-datetime [x]))

(extend-protocol ToDatetime
  java.util.Date
  (to-datetime [x] x)
  String
  (to-datetime [x] (instant/read-instant-date x))
  Number
  (to-date [x] (java.util.Date. (long x)))
  DateTime
  (to-date [x] (.toDate x))
  LocalDate
  (to-date [x] (.toDate x)))


;; For data-source-wide marshalling/unmarshalling. Implemented as protocols
;; for performance

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

