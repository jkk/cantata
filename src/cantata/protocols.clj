(ns cantata.protocols
  (:require [clojure.instant :as instant])
  (:import org.joda.time.DateTimeZone
           org.joda.time.LocalDate
           org.joda.time.LocalTime
           org.joda.time.DateTime))

(set! *warn-on-reflection* true)

(defprotocol ParseInt
  (parse-int [x]))

(extend-protocol ParseInt
  Number
  (parse-int [x] (long x))
  String
  (parse-int [x] (Long/valueOf x))
  Character
  (parse-int [x] (Character/getNumericValue x))
  java.util.Date
  (parse-int [x] (.getTime x))
  DateTime
  (parse-int [x] (.getMillis x))
  LocalDate
  (parse-int [x] (.getTime ^java.util.Date (.toDate x)))
  nil
  (parse-int [_] nil))

(defprotocol ParseStr
  (parse-str [x]))

(extend-protocol ParseStr
  String
  (parse-str [x] x)
  java.sql.Clob
  (parse-str [x] (slurp (.getCharacterStream x)))
  java.sql.Blob
  (parse-str [x] (parse-str (.getBytes x 0 (.length x))))
  Character
  (parse-str [x] (str x))
  Object
  (parse-str [x] (pr-str x)) ;NOT using str here
  nil
  (parse-str [_] nil))

(extend (Class/forName "[B") ;byte arrays
  ParseStr
  {:parse-str (fn [^"[B" x] (String. x "UTF-8"))})

(defprotocol ParseDouble
  (parse-double [x]))

(extend-protocol ParseDouble
  Number
  (parse-double [x] (double x))
  String
  (parse-double [x] (Double/valueOf x))
  Character
  (parse-double [x] (parse-double (Character/getNumericValue x)))
  nil
  (parse-double [_] nil))

(defprotocol ParseDecimal
  (parse-decimal [x]))

(extend-protocol ParseDecimal
  Number
  (parse-decimal [x] (bigdec x))
  String
  (parse-decimal [x] (bigdec x))
  nil
  (parse-double [_] nil))

(defprotocol ParseBoolean
  (parse-boolean [x]))

(extend-protocol ParseBoolean
  Number
  (parse-boolean [x] (not (zero? x)))
  String
  (parse-boolean [x] (Boolean/valueOf x))
  nil
  (parse-boolean [_] nil))

(defprotocol ParseBytes
  (parse-bytes [x]))

(extend-protocol ParseBytes
  String
  (parse-bytes [x] (.getBytes x "UTF-8"))
  java.sql.Blob
  (parse-bytes [x] (.getBytes x 0 (.length x)))
  nil
  (parse-bytes [_] nil))

(extend (Class/forName "[B") ;byte arrays
  ParseBytes
  {:parse-bytes (fn [x] x)})

(defprotocol ParseDate
  (parse-date [x]))

(def date-sdf (java.text.SimpleDateFormat. "yyyy-MM-dd"))

(extend-protocol ParseDate
  java.util.Date
  (parse-date [x] x)
  String
  (parse-date [x] (.parse ^java.text.SimpleDateFormat date-sdf x))
  Number
  (parse-date [x] (java.util.Date. (long x)))
  DateTime
  (parse-date [x] (.toDate x))
  LocalDate
  (parse-date [x] (.toDate x))
  nil
  (parse-date [_] nil))

(defprotocol ParseJodaDate
  (parse-joda-date [x]))

(extend-protocol ParseJodaDate
  java.util.Date
  (parse-joda-date [x] (LocalDate. (.getTime x) DateTimeZone/UTC))
  String
  (parse-joda-date [x] (LocalDate/parse x))
  Number
  (parse-joda-date [x] (LocalDate. (long x) DateTimeZone/UTC))
  nil
  (parse-joda-date [_] nil))

(defprotocol ParseTime
  (parse-time [x]))

(def time-sdf (java.text.SimpleDateFormat. "HH:mm:ss"))

(extend-protocol ParseTime
  java.util.Date
  (parse-time [x] x)
  String
  (parse-time [x] (.parse ^java.text.SimpleDateFormat time-sdf x))
  Number
  (parse-time [x] (java.util.Date. (long x)))
  DateTime
  (parse-time [x] (.toDate x))
  LocalTime
  (parse-time [x] (.toDate ^DateTime (.toDateTimeToday x)))
  nil
  (parse-time [_] nil))

(defprotocol ParseJodaTime
  (parse-joda-time [x]))

(extend-protocol ParseJodaTime
  java.util.Date
  (parse-joda-time [x] (LocalTime. (.getTime x) DateTimeZone/UTC))
  String
  (parse-joda-time [x] (LocalTime/parse x))
  Number
  (parse-joda-time [x] (LocalTime. (long x) DateTimeZone/UTC))
  nil
  (parse-joda-time [_] nil))

(defprotocol ParseDatetime
  (parse-datetime [x]))

(extend-protocol ParseDatetime
  java.util.Date
  (parse-datetime [x] x)
  String
  (parse-datetime [x] (instant/read-instant-date x))
  Number
  (parse-datetime [x] (java.util.Date. (long x)))
  DateTime
  (parse-datetime [x] (.toDate x))
  LocalDate
  (parse-datetime [x] (.toDate x))
  nil
  (parse-datetime [_] nil))

(defprotocol ParseJodaDatetime
  (parse-joda-datetime [x]))

(extend-protocol ParseJodaDatetime
  java.util.Date
  (parse-joda-datetime [x] (DateTime. (.getTime x) DateTimeZone/UTC))
  String
  (parse-joda-datetime [x] (DateTime.
                             (.getTime
                               ^java.util.Date (instant/read-instant-date x))
                             DateTimeZone/UTC))
  Number
  (parse-joda-datetime [x] (DateTime. (long x) DateTimeZone/UTC))
  DateTime
  (parse-joda-datetime [x] x)
  LocalDate
  (parse-joda-datetime [x] (.toDateTimeAtStartOfDay x DateTimeZone/UTC))
  nil
  (parse-joda-datetime [_] nil))


;; For data-source-wide marshalling/unmarshalling. Implemented as protocols
;; for performance

(defprotocol ClobToStr
  (clob->str [x]))

(extend-protocol ClobToStr
  java.sql.Clob (clob->str [x] (slurp (.getCharacterStream x)))
  Object (clob->str [x] x)
  nil (clob->str [_] nil))

(defprotocol BlobToBytes
  (blob->bytes [x]))

(extend-protocol BlobToBytes
  java.sql.Blob (blob->bytes [x] (.getBytes x 0 (.length x)))
  Object (blob->bytes [x] x)
  nil (blob->bytes [_] nil))

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

