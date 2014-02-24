(ns cantata.protocols
  "Protocols and implementations for data type parsing and marshalling"
  (:require [clojure.instant :as instant])
  (:import org.joda.time.DateTimeZone
           org.joda.time.LocalDate
           org.joda.time.LocalTime
           org.joda.time.DateTime))

(set! *warn-on-reflection* true)

(defprotocol ParseInt
  (parse-int [x]))

(extend-protocol ParseInt
  Long
  (parse-int [x] x)
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
  Double
  (parse-double [x] x)
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
  BigDecimal
  (parse-decimal [x] x)
  Number
  (parse-decimal [x] (bigdec x))
  String
  (parse-decimal [x] (bigdec x))
  nil
  (parse-decimal [_] nil))

(defprotocol ParseBoolean
  (parse-boolean [x]))

(extend-protocol ParseBoolean
  Boolean
  (parse-boolean [x] x)
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
  LocalDate
  (parse-joda-date [x] x)
  DateTime
  (parse-joda-date [x] (.toLocalDate x))
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
  DateTime
  (parse-joda-time [x] (.toLocalTime x))
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

;;;;

(defprotocol MarshalInt
  (marshal-int [x]))

(extend-protocol MarshalInt
  Long
  (marshal-int [x] x)
  Number
  (marshal-int [x] (long x))
  String
  (marshal-int [x] (Long/valueOf x))
  Character
  (marshal-int [x] (Character/getNumericValue x))
  java.util.Date
  (marshal-int [x] (.getTime x))
  DateTime
  (marshal-int [x] (.getMillis x))
  LocalDate
  (marshal-int [x] (.getTime ^java.util.Date (.toDate x)))
  nil
  (marshal-int [_] nil))

(defprotocol MarshalStr
  (marshal-str [x]))

(extend-protocol MarshalStr
  String
  (marshal-str [x] x)
  java.sql.Clob
  (marshal-str [x] (slurp (.getCharacterStream x)))
  java.sql.Blob
  (marshal-str [x] (marshal-str (.getBytes x 0 (.length x))))
  Character
  (marshal-str [x] (str x))
  Object
  (marshal-str [x] (pr-str x)) ;NOT using str here
  nil
  (marshal-str [_] nil))

(extend (Class/forName "[B") ;byte arrays
  MarshalStr
  {:marshal-str (fn [^"[B" x] (String. x "UTF-8"))})

(defprotocol MarshalDouble
  (marshal-double [x]))

(extend-protocol MarshalDouble
  Double
  (marshal-double [x] x)
  Number
  (marshal-double [x] (double x))
  String
  (marshal-double [x] (Double/valueOf x))
  Character
  (marshal-double [x] (marshal-double (Character/getNumericValue x)))
  nil
  (marshal-double [_] nil))

(defprotocol MarshalDecimal
  (marshal-decimal [x]))

(extend-protocol MarshalDecimal
  BigDecimal
  (marshal-decimal [x] x)
  Number
  (marshal-decimal [x] (bigdec x))
  String
  (marshal-decimal [x] (bigdec x))
  nil
  (marshal-decimal [_] nil))

(defprotocol MarshalBoolean
  (marshal-boolean [x]))

(extend-protocol MarshalBoolean
  Boolean
  (marshal-boolean [x] x)
  Number
  (marshal-boolean [x] (not (zero? x)))
  String
  (marshal-boolean [x] (Boolean/valueOf x))
  nil
  (marshal-boolean [_] nil))

(defprotocol MarshalBytes
  (marshal-bytes [x]))

(extend-protocol MarshalBytes
  String
  (marshal-bytes [x] (.getBytes x "UTF-8"))
  java.sql.Blob
  (marshal-bytes [x] (.getBytes x 0 (.length x)))
  nil
  (marshal-bytes [_] nil))

(extend (Class/forName "[B") ;byte arrays
  MarshalBytes
  {:marshal-bytes (fn [x] x)})

(defprotocol MarshalDate
  (marshal-date [x]))

(def date-sdf (java.text.SimpleDateFormat. "yyyy-MM-dd"))

(defn date->sql-timestamp [^java.util.Date d]
  (java.sql.Timestamp. (.getTime d)))

(defn date->sql-date [^java.util.Date d]
  (java.sql.Date. (.getTime d)))

(defn date->sql-time [^java.util.Date d]
  (java.sql.Time. (.getTime d)))

(extend-protocol MarshalDate
  java.util.Date
  (marshal-date [x] (date->sql-date x))
  String
  (marshal-date [x] (date->sql-date
                      (.parse ^java.text.SimpleDateFormat date-sdf x)))
  Number
  (marshal-date [x] (date->sql-date (java.util.Date. (long x))))
  DateTime
  (marshal-date [x] (date->sql-date (.toDate x)))
  LocalDate
  (marshal-date [x] (date->sql-date (.toDate x)))
  nil
  (marshal-date [_] nil))

(defprotocol MarshalTime
  (marshal-time [x]))

(def time-sdf (java.text.SimpleDateFormat. "HH:mm:ss"))

(extend-protocol MarshalTime
  java.util.Date
  (marshal-time [x] (date->sql-time x))
  String
  (marshal-time [x] (date->sql-time (.parse ^java.text.SimpleDateFormat time-sdf x)))
  Number
  (marshal-time [x] (date->sql-time (java.util.Date. (long x))))
  DateTime
  (marshal-time [x] (date->sql-time (.toDate x)))
  LocalTime
  (marshal-time [x] (date->sql-time (.toDate ^DateTime (.toDateTimeToday x))))
  nil
  (marshal-time [_] nil))

(defprotocol MarshalDatetime
  (marshal-datetime [x]))

(extend-protocol MarshalDatetime
  java.util.Date
  (marshal-datetime [x] (date->sql-timestamp x))
  String
  (marshal-datetime [x] (date->sql-timestamp (instant/read-instant-date x)))
  Number
  (marshal-datetime [x] (date->sql-timestamp (java.util.Date. (long x))))
  DateTime
  (marshal-datetime [x] (date->sql-timestamp (.toDate x)))
  LocalDate
  (marshal-datetime [x] (date->sql-timestamp (.toDate x)))
  nil
  (marshal-datetime [_] nil))


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

