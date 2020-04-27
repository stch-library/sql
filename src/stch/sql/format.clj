(ns stch.sql.format
  "Format query and DML statements. Use with stch.sql."
  (:refer-clojure :exclude [format])
  (:require
   [clojure.string :as string]
   [stch.sql.types :refer [call raw param param-name]]
   [stch.sql.util :refer :all])
  (:import
   (stch.sql.types
    SqlCall
    SqlParam
    SqlRaw)))

(def ^:dynamic *clause*
  "During formatting, *clause* is bound to :select, :from, :where, etc."
  nil)

(def ^:dynamic *params*
  "Will be bound to an atom-vector that accumulates SQL parameters across
  possibly-recursive function calls"
  nil)

(def ^:dynamic *param-names* nil)

(def ^:dynamic *param-counter* nil)

(def ^:dynamic *input-params* nil)

(def ^:dynamic *fn-context?* false)

(def ^:dynamic *subquery?* false)

(def ^:private quote-fns
  {:ansi #(str \" % \")
   :mysql #(str \` % \`)
   :sqlserver #(str \[ % \])
   :oracle #(str \" % \")})

(def ^:dynamic *quote-identifier-fn* nil)

(defn quote-identifier [x & {:keys [style split] :or {split true}}]
  (let [qf (if style
             (quote-fns style)
             *quote-identifier-fn*)
        s (cond
            (or (keyword? x) (symbol? x)) (undasherize (name x))
            (string? x) (if qf x (undasherize x))
            :else (str x))]
    (if-not qf
      s
      (let [qf* #(if (= "*" %) % (qf %))]
        (if-not split
          (qf* s)
          (let [parts (string/split s #"\.")]
            (string/join "." (map qf* parts))))))))

(def infix-fns
  #{"+" "-" "*" "/" "%" "mod" "|" "&" "^"
    "and" "or" "xor" "in" "not in" "like"
    "not like" "regexp" "not regexp"})

(def fn-aliases
  {"is" "="
   "is-not" "<>"
   "not=" "<>"
   "!=" "<>"
   "not-in" "not in"
   "not-like" "not like"
   "regex" "regexp"
   "not-regex" "not regexp"
   "not-regexp" "not regexp"})

(declare to-sql format-predicate*)

(defmulti fn-handler (fn [op & args] op))

(defn expand-binary-ops [op & args]
  (str "("
       (string/join " AND "
                    (for [[a b] (partition 2 1 args)]
                      (fn-handler op a b)))
       ")"))

(defmethod fn-handler :default [op & args]
  (let [args (map to-sql args)]
    (if (infix-fns op)
      (let [op (string/upper-case op)]
        (paren-wrap (string/join (str " " op " ") args)))
      (str op (paren-wrap (comma-join args))))))

(defmethod fn-handler "count-distinct" [_ & args]
  (str "COUNT(DISTINCT " (comma-join (map to-sql args)) ")"))

(defmethod fn-handler "distinct-on" [_ & args]
  (str "DISTINCT ON (" (comma-join (map to-sql args)) ")"))

(defmethod fn-handler "=" [_ a b & more]
  (if (seq more)
    (apply expand-binary-ops "=" a b more)
    (cond
      (nil? a) (str (to-sql b) " IS NULL")
      (nil? b) (str (to-sql a) " IS NULL")
      :else (str (to-sql a) " = " (to-sql b)))))

(defmethod fn-handler "<>" [_ a b & more]
  (if (seq more)
    (apply expand-binary-ops "<>" a b more)
    (cond
      (nil? a) (str (to-sql b) " IS NOT NULL")
      (nil? b) (str (to-sql a) " IS NOT NULL")
      :else (str (to-sql a) " <> " (to-sql b)))))

(defmethod fn-handler "<" [_ a b & more]
  (if (seq more)
    (apply expand-binary-ops "<" a b more)
    (str (to-sql a) " < " (to-sql b))))

(defmethod fn-handler "<=" [_ a b & more]
  (if (seq more)
    (apply expand-binary-ops "<=" a b more)
    (str (to-sql a) " <= " (to-sql b))))

(defmethod fn-handler ">" [_ a b & more]
  (if (seq more)
    (apply expand-binary-ops ">" a b more)
    (str (to-sql a) " > " (to-sql b))))

(defmethod fn-handler ">=" [_ a b & more]
  (if (seq more)
    (apply expand-binary-ops ">=" a b more)
    (str (to-sql a) " >= " (to-sql b))))

(defmethod fn-handler "between" [_ field lower upper]
  (str (to-sql field) " BETWEEN "
       (to-sql lower) " AND " (to-sql upper)))

(defmethod fn-handler "not-between" [_ field lower upper]
  (str (to-sql field) " NOT BETWEEN "
       (to-sql lower) " AND " (to-sql upper)))

;; Handles MySql's MATCH (field) AGAINST (pattern). The third argument
;; can be a set containing one or more of :boolean, :natural, or :expand.
(defmethod fn-handler "match" [_ fields pattern & [opts]]
  (str "MATCH ("
       (comma-join
        (map to-sql (if (coll? fields) fields [fields])))
       ") AGAINST ("
       (to-sql pattern)
       (when (seq opts)
         (str " " (space-join (for [opt opts]
                                (condp = opt
                                  :boolean "IN BOOLEAN MODE"
                                  :natural "IN NATURAL LANGUAGE MODE"
                                  :expand "WITH QUERY EXPANSION")))))
       ")"))

(def clause-order
  "Determines the order that clauses will be placed within generated SQL"
  [:insert-into :replace-into :update :delete-from :using
   :columns :set :union :select :from :join :left-join
   :right-join :where :group-by :having :order-by
   :limit :offset :values :query-values :on-dup-key])

(def known-clauses (set clause-order))

(defn format
  "Takes a SQL map and optional input parameters and returns a vector
  of a SQL string and parameters, as expected by clojure.java.jdbc.

  Input parameters will be filled into designated spots according to
  name (if a map is provided) or by position (if a sequence is provided).

  Instead of passing parameters, you can use keyword arguments:
    :params - input parameters
    :quoting - quote style to use for identifiers; one of :ansi (PostgreSQL),
               :mysql, :sqlserver, or :oracle. Defaults to no quoting.
    :return-param-names - when true, returns a vector of
                          [sql-str param-values param-names]"
  [sql-map & params-or-opts]
  (let [opts (when (keyword? (first params-or-opts))
               (apply hash-map params-or-opts))
        params (if (coll? (first params-or-opts))
                 (first params-or-opts)
                 (:params opts))]
    (binding [*params* (atom [])
              *param-counter* (atom 0)
              *param-names* (atom [])
              *input-params* (atom params)
              *quote-identifier-fn* (quote-fns (:quoting opts))]
      (let [sql-str (to-sql sql-map)]
        (if (seq @*params*)
          (if (:return-param-names opts)
            [sql-str @*params* @*param-names*]
            (into [sql-str] @*params*))
          [sql-str])))))

(defn format-predicate
  "Formats a predicate (e.g., for WHERE, JOIN, or HAVING) as a string."
  [pred & {:keys [quoting]}]
  (binding [*params* (atom [])
            *param-counter* (atom 0)
            *param-names* (atom [])
            *quote-identifier-fn* (or (quote-fns quoting)
                                      *quote-identifier-fn*)]
    (let [sql-str (format-predicate* pred)]
      (if (seq @*params*)
        (into [sql-str] @*params*)
        [sql-str]))))

(defprotocol ToSQL
  (-to-sql [x]))

(declare -format-clause)

(extend-protocol ToSQL
  clojure.lang.Keyword
  (-to-sql [x]
    (let [s ^String (name x)]
      (if (= (.charAt s 0) \?)
        (to-sql (param (keyword (subs s 1))))
        (quote-identifier x))))

  clojure.lang.Symbol
  (-to-sql [x]
    (let [s ^String (name x)]
      (if (= (.charAt s 0) \?)
        (to-sql (param (keyword (subs s 1))))
        (quote-identifier x))))

  java.lang.Number
  (-to-sql [x] (str x))

  java.lang.Boolean
  (-to-sql [x] (if x "TRUE" "FALSE"))

  clojure.lang.IPersistentVector
  (-to-sql [x]
    (if *fn-context?*
      ;; list argument in fn call
      (paren-wrap (comma-join (map to-sql x)))
      ;; alias
      (str (to-sql (first x))
           " AS "
           (if (string? (second x))
             (quote-identifier (second x))
             (to-sql (second x))))))

  clojure.lang.IPersistentList
  (-to-sql [x]
    (binding [*fn-context?* true]
      (let [fn-name (name (first x))
            fn-name (fn-aliases fn-name fn-name)]
        (apply fn-handler fn-name (rest x)))))

  SqlCall
  (-to-sql [x]
    (binding [*fn-context?* true]
      (let [fn-name (name (.name x))
            fn-name (fn-aliases fn-name fn-name)]
        (apply fn-handler fn-name (.args x)))))

  SqlRaw
  (-to-sql [x] (.s x))

  clojure.lang.IPersistentMap
  (-to-sql [x]
    (let [clause-ops (filter #(contains? x %) clause-order)
          sql-str
          (binding [*subquery?* true
                    *fn-context?* false]
            (space-join
             (map (comp #(-format-clause % x) #(find x %))
                  clause-ops)))]
      (if *subquery?*
        (paren-wrap sql-str)
        sql-str)))

  nil
  (-to-sql [x] "NULL"))

(defn sqlable? [x]
  (satisfies? ToSQL x))

(defn to-sql [x]
  (if (satisfies? ToSQL x)
    (-to-sql x)
    (let [[x pname]
          (if (instance? SqlParam x)
            (let [pname (param-name x)]
              (if (map? @*input-params*)
                [(get @*input-params* pname) pname]
                (let [x (first @*input-params*)]
                  (swap! *input-params* rest)
                  [x pname])))
            ;; Anonymous param name -- :_1, :_2, etc.
            [x (keyword (str "_" (swap! *param-counter* inc)))])]
      (swap! *param-names* conj pname)
      (swap! *params* conj x)
      "?")))

;;;;

(defn format-predicate* [pred]
  (if-not (sequential? pred)
    (to-sql pred)
    (let [[op & args] pred
          op-name (name op)]
      (if (= "not" op-name)
        (str "NOT " (format-predicate* (first args)))
        (if (#{"and" "or" "xor"} op-name)
          (paren-wrap
           (string/join (str " " (string/upper-case op-name) " ")
                        (map format-predicate* args)))
          (to-sql (apply call pred)))))))

(defn- format-modifiers [sql-map]
  (when (:modifiers sql-map)
    (str (space-join (map (comp string/upper-case name)
                          (:modifiers sql-map)))
         " ")))

(defmulti format-clause
  "Takes a map entry representing a clause and returns an SQL string"
  (fn [clause _] (key clause)))

(defn- -format-clause
  [clause _]
  (binding [*clause* (key clause)]
    (format-clause clause _)))

(defmethod format-clause :default [& _]
  "")

(defmethod format-clause :union [[_ select-stmts] _]
  (string/join " UNION " (map to-sql select-stmts)))

(defmethod format-clause :select [[_ fields] sql-map]
  (str "SELECT " (format-modifiers sql-map)
       (comma-join (map to-sql fields))))

(defmethod format-clause :from [[_ tables] _]
  (str "FROM " (comma-join (map to-sql tables))))

(defmethod format-clause :where [[_ pred] _]
  (str "WHERE " (format-predicate* pred)))

(defn format-join [type table pred]
  (str (when type
         (str (string/upper-case (name type)) " "))
       "JOIN " (to-sql table)
       " ON " (format-predicate* pred)))

(defmethod format-clause :join [[_ join-groups] _]
  (space-join (map #(apply format-join :inner %)
                   (partition 2 join-groups))))

(defmethod format-clause :left-join [[_ join-groups] _]
  (space-join (map #(apply format-join :left %)
                   (partition 2 join-groups))))

(defmethod format-clause :right-join [[_ join-groups] _]
  (space-join (map #(apply format-join :right %)
                   (partition 2 join-groups))))

(defmethod format-clause :group-by [[_ fields] _]
  (str "GROUP BY " (comma-join (map to-sql fields))))

(defmethod format-clause :having [[_ pred] _]
  (str "HAVING " (format-predicate* pred)))

(defmethod format-clause :order-by [[_ fields] _]
  (str "ORDER BY "
       (comma-join
        (for [field fields]
          (if (vector? field)
            (let [[field order] field]
              (str (to-sql field) " "
                   (if (= "desc" (name order))
                     "DESC" "ASC")))
            (to-sql field))))))

(defmethod format-clause :limit [[_ limit] _]
  (str "LIMIT " (to-sql limit)))

(defmethod format-clause :offset [[_ offset] _]
  (str "OFFSET " (to-sql offset)))

(defmethod format-clause :insert-into [[_ table] sql-map]
  (str "INSERT " (format-modifiers sql-map)
       "INTO " (to-sql table)))

(defmethod format-clause :replace-into [[_ table] sql-map]
  (str "REPLACE " (format-modifiers sql-map)
       "INTO " (to-sql table)))

(defmethod format-clause :columns [[_ fields] _]
  (str "(" (comma-join (map to-sql fields)) ")"))

(defmethod format-clause :values [[_ values] _]
  (if (sequential? (first values))
    (str "VALUES "
         (comma-join
          (for [x values]
            (str "(" (comma-join (map to-sql x)) ")"))))
    (str
     "(" (comma-join (map to-sql (keys (first values)))) ") VALUES "
     (comma-join (for [x values]
                   (str "(" (comma-join (map to-sql (vals x))) ")"))))))

(defmethod format-clause :query-values [[_ query-values] _]
  (to-sql query-values))

(defmethod format-clause :update [[_ tables] sql-map]
  (str "UPDATE " (format-modifiers sql-map)
       (comma-join (map to-sql tables))))

(defmethod format-clause :set [[_ values] _]
  (str "SET " (comma-join (for [[k v] values]
                            (str (to-sql k) " = " (to-sql v))))))

(defmethod format-clause :on-dup-key [[_ values] _]
  (str "ON DUPLICATE KEY UPDATE "
       (comma-join (for [[k v] values]
                     (str (to-sql k) " = " (to-sql v))))))

(defmethod format-clause :delete-from [[_ tables] sql-map]
  (str "DELETE " (format-modifiers sql-map)
       "FROM " (comma-join (map to-sql tables))))

(defmethod format-clause :using [[_ tables] _]
  (str "USING " (comma-join (map to-sql tables))))
