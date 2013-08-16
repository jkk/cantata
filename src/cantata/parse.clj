(ns cantata.parse
  (:require [cantata.protocols :as cp]))

;; For custom types
(defmulti parse-value
  "Parses a value according to the given type, for use in an application.
  The value can be anything that might reasonably be parsed into the type.
  Example:

    (parse-value \"123\" :double) => 123.0

  Built-in types:

    :int :str :boolean :double :decimal :bytes
    :datetime :date :time

  You can extend parsing to built-in types by extending relevant protocols
  in the cantata.protocols namespace. 

  Parsing an unrecognized type will pass the value back unparsed.

  Add your own types with defmethod. Dispatches on the type argument."
  (fn [v type] type))

(defmethod parse-value :default [v type]
  (case type
    :int (cp/parse-int v)
    :str (cp/parse-str v)
    :boolean (cp/parse-boolean v)
    :datetime (cp/parse-datetime v)
    :date (cp/parse-date v)
    :time (cp/parse-time v)
    :double (cp/parse-double v)
    :decimal (cp/parse-decimal v)
    :bytes (cp/parse-bytes v)
    v))

(defmulti marshal-value
  "Parses a value according to the given type, for the purpose of storage
  in a data source. Same as `parse-value` for built-in types, but could differ
  for custom types."
  (fn [v type] type))

(defmethod marshal-value :default [v type]
  (case type
    :int (cp/parse-int v)
    :str (cp/parse-str v)
    :boolean (cp/parse-boolean v)
    :datetime (cp/parse-datetime v)
    :date (cp/parse-date v)
    :time (cp/parse-time v)
    :double (cp/parse-double v)
    :decimal (cp/parse-decimal v)
    :bytes (cp/parse-bytes v)
    v))

;;;;

(def ^:private op-chars #{\( \) \: \< \> \= \! \~ \space \,})

;; TODO: in
(def ^:private ops
  {":" #(vector :like %1 (str "%" %2 "%"))
   "=" :=
   "!=" :not=
   "<>" :not=
   ">" :>
   ">=" :>=
   "<" :<
   "<=" :<=})

(defn ^:private char-at [^String s i]
  (when (< i (.length s))
    (.charAt s i)))

(defn ^:private tokenize [^String s]
  (let [tok-meta (atom {})]
    (loop [i 0, toks []]
      (if (= i (.length s))
        (with-meta toks @tok-meta)
        (let [c (.charAt s i)]
          (cond
           (= \" c) (let [[i tok] (loop [i (inc i), sb (StringBuilder.)]
                                    (let [c (char-at s i)]
                                      (cond
                                       (not c) [i (str sb)]
                                       (= \" c) [(inc i) (str sb)]
                                       (= \\ c) (recur (+ i 2) (.append sb (char-at s (inc i))))
                                       :else (recur (inc i) (.append sb c)))))]
                      (swap! tok-meta assoc (count toks) #{:quoted})
                      (recur i (conj toks tok)))
           (#{\( \)} c) (recur (inc i) (conj toks (str c)))
           (#{\space \,} c) (recur (inc i) toks)
           (op-chars c) (let [[i tok] (loop [i i, sb (StringBuilder.)]
                                        (let [c (char-at s i)]
                                          (if (or (not c)
                                                  (= \space c)
                                                  (not (op-chars c)))
                                            [i (str sb)]
                                            (recur (inc i) (.append sb c)))))]
                          (recur i (conj toks tok)))
           :else (let [[i tok] (loop [i i, sb (StringBuilder.)]
                                 (let [c (char-at s i)]
                                   (if (or (not c)
                                           (op-chars c))
                                     [i (str sb)]
                                     (recur (inc i) (.append sb c)))))]
                   (recur i (conj toks tok)))))))))

(defn ^:private join-clauses [logic-op clauses]
  (if (= 1 (count clauses))
    (first clauses)
    (into [logic-op] clauses)))

(defn ^:private parse-op-clause [tok i tokens types]
  (when (< (inc i) (count tokens))
    (when-let [op (ops (nth tokens (inc i)))]
      (let [field (keyword tok)
            val (nth tokens (+ i 2))
            val-meta (get (meta tokens) (+ i 2))
            val (if (and (or (= "nil" val) (= "null" val))
                         (not (:quoted val-meta)))
                  nil val)
            val (if-let [type (types field)]
                  (parse-value val type)
                  val)]
        (if (fn? op)
          (op field val)
          (if (= "" val)
            (let [logic-op (if (= :not= op)
                             :and :or)]
              [logic-op [op field val] [op field nil]])
            [op field val]))))))

(defn ^:private parse-predicate*
  [tokens i logic-op types]
  (loop [i i
         logic-op logic-op
         clauses []]
    (if (>= i (count tokens))
      [i (join-clauses logic-op clauses)]
      (let [tok (nth tokens i)]
        (if (= ")" tok)
          [(inc i) (join-clauses logic-op clauses)]
          (if (#{"and" "or" "xor" "not"} tok)
            (recur (inc i) (keyword tok) clauses)
            (if (= "(" tok)
              (let [[i* clauses*] (parse-predicate* tokens (inc i) :and types)]
                (recur i* logic-op (conj clauses clauses*)))
              (if-let [op-clause (parse-op-clause tok i tokens types)]
                (recur (+ i 3) logic-op (conj clauses op-clause))
                (recur (inc i) logic-op
                       (conj clauses ((ops ":") :* tok)))))))))))

;; TODO: take into account field types
(defn parse-predicate
  "Parses a string like \"foo = bar baz-quux = nil\" into a vector
  predicate like [:and [:= :foo \"bar\"] [:= :baz-quux nil]].

  Types is a map of field keyword name to data type keyword name;
  see `parse-value` for accepted types.

  Supported operators: = != <> < <= > >= :

  The \":\" operator expands like so:

    \"foo: bar\" => [:like :foo \"%bar%\"]

  Supported logical operators: and or xor not

  Terms can be grouped with parentheses.

  A string without operators is parsed like so:

    \"foo\" => [:like :* \"%foo%\"]

  Double quotes can be used to delimit terms with multiple words.

  The strings \"null\" and \"nil\" are parsed as nil, unless delimited with
  double quotes:

    \"foo = nil\"          => [:= :foo nil]
    \"foo = \\\"nil\\\"\"  => [:= :foo \"nil\"]

  Two successive double quotes will be expanded to also check for nil, like so:

    \"foo = \\\"\\\"\" => [:or [:= :foo \"\"] [:= :foo nil]]"
  ([s]
    (parse-predicate s {}))
  ([s types]
    (let [[_ pred] (parse-predicate* (tokenize s) 0 :and types)]
      pred)))