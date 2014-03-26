(ns stch.sql.ddl
  "SQL DSL for data definition language (DDL).
  Supports a majority of MySQL's statements for
  tables and databases."
  (:require [clojure.string :as string])
  (:use [stch.sql.util]
        [stch.util :only [named?]]
        [stch.schema]))

(def ^:private special-keywords
  {"auto-increment" "AUTO_INCREMENT"})

(def ^:private parse-identifier
  (comp undasherize name))

(def ^{:private true :dynamic true} *op*)

(defn- parse-keyword
  "Parse a SQL keyword."
  [s]
  (-> (string/split s #"-")
      space-join
      string/upper-case))

(defprotocol ToSQL
  "Convert an object to SQL."
  (to-sql [x]))

(extend-protocol ToSQL
  clojure.lang.Named
  (to-sql [kw]
    (let [kw (name kw)]
      (special-keywords kw (parse-keyword kw))))

  Number
  (to-sql [x] x)

  String
  (to-sql [x] (str \' x \'))

  clojure.lang.Sequential
  (to-sql [x]
    (-> (map to-sql x)
        comma-join
        paren-wrap)))

(defprotocol WhatsMyName
  (whats-my-name [x]))

(extend-protocol WhatsMyName
  clojure.lang.Named
  (whats-my-name [x] (name x))

  String
  (whats-my-name [x] x))

(def ^:private parse-name
  (comp parse-identifier whats-my-name))

(defprotocol AlterAdd
  (-add [spec m] [spec m options]))

(defrecord' DBSpec
  [db-name :- Named]

  WhatsMyName
  (whats-my-name [_] db-name)

  ToSQL
  (to-sql [_]
    (str "DATABASE "
         (parse-identifier db-name))))

(defn' db :- DBSpec
  "Create a database."
  [db-name :- Named]
  (->DBSpec db-name))

(defn' db? :- Boolean
  [x :- Any]
  (instance? DBSpec x))

(declare ->AddColumnSpec)

(defrecord' DefaultValueSpec
  [x :- (U Number String)]

  ToSQL
  (to-sql [_]
    (str "DEFAULT " (to-sql x))))

(defn' default :- DefaultValueSpec
  "Set the default value for a column.
  Use with a column fn."
  [x :- (U Number String)]
  (->DefaultValueSpec x))

(def KeywordOrSym
  "Keyword or symbol type definition."
  (U Keyword Symbol))

(def ColumnOptions
  "Column options type definition."
  (U [(One (U [Int] [String]))
      (U KeywordOrSym DefaultValueSpec)]
     [(U KeywordOrSym DefaultValueSpec)]))

(defrecord' ColumnSpec
  [col-name :- Named
   col-type :- String
   options :- ColumnOptions]

  WhatsMyName
  (whats-my-name [_] col-name)

  AlterAdd
  (-add [spec m] (-add spec m nil))
  (-add [spec m options]
    (->> (->AddColumnSpec spec options)
         (update-in m [:columns+keys] conj)))

  ToSQL
  (to-sql [_]
    (let [first-opt-seq? (sequential? (first options))]
      (space-join
        (list* (parse-identifier col-name)
               (if first-opt-seq?
                 (str col-type (to-sql (first options)))
                 col-type)
               (if first-opt-seq?
                 (map to-sql (rest options))
                 (map to-sql options)))))))

(def NamedOrColumn
  "Named or column spec type definition."
  (U Named ColumnSpec))

(defn' column? :- Boolean
  [x :- Any]
  (instance? ColumnSpec x))

(defrecord' ColumnPosition
  [preposition :- String
   col :- NamedOrColumn]

  ToSQL
  (to-sql [_]
    (str preposition " " (parse-name col))))

(defn' after :- ColumnPosition
  "Set column position. Use with add."
  [col :- NamedOrColumn]
  (->ColumnPosition "AFTER" col))

(defrecord' AddColumnSpec
  [col-spec :- ColumnSpec
   col-position :- (U KeywordOrSym ColumnPosition)]

  ToSQL
  (to-sql [_]
    (str "ADD COLUMN " (to-sql col-spec)
         (when col-position
           (str " " (to-sql col-position))))))

(defn' column :- ColumnSpec
  [col-name col-type options]
  (->ColumnSpec col-name col-type options))

(defmacro deftypefn
  "Define a type fn (e.g., int, varchar)."
  [fn-name col-type]
  `(defn ~fn-name
     [& [arg0# & args#]]
     (cond (map? arg0#)
           (let [col# (column (first args#)
                              ~col-type
                              (rest args#))]
             (update-in arg0# [:columns+keys] conj col#))
           (named? arg0#)
           (column arg0# ~col-type args#))))

; Numeric
(deftypefn bool "BOOL")
(deftypefn tiny-int "TINYINT")
(deftypefn small-int "SMALLINT")
(deftypefn medium-int "MEDIUMINT")
(deftypefn integer "INT")
(deftypefn big-int "BIGINT")
(deftypefn decimal "DECIMAL")
(deftypefn float' "FLOAT")
(deftypefn double' "DOUBLE")
; String
(deftypefn chr "CHAR")
(def char' chr)
(deftypefn varchar "VARCHAR")
(deftypefn binary "BINARY")
(deftypefn varbinary "VARBINARY")
(deftypefn blob "BLOB")
(deftypefn text "TEXT")
(deftypefn enum "ENUM")
(deftypefn set' "SET")
; Date and time
(deftypefn date "DATE")
(deftypefn datetime "DATETIME")
(deftypefn timestamp "TIMESTAMP")
(deftypefn time' "TIME")
(deftypefn year "YEAR")
; Spatial
(deftypefn geometry "GEOMETRY")
(deftypefn point "POINT")
(deftypefn linestring "LINESTRING")
(deftypefn polygon "POLYGON")

(declare ->AddIndexSpec)

(defrecord' IndexSpec
  [index-type :- String
   index-cols :- (U [NamedOrColumn] NamedOrColumn)]

  AlterAdd
  (-add [spec m]
    (->> (->AddIndexSpec spec)
         (update-in m [:columns+keys] conj)))

  ToSQL
  (to-sql [_]
    (str index-type
         (paren-wrap
           (if (sequential? index-cols)
             (-> (map parse-name index-cols)
                 comma-join)
             (parse-name index-cols))))))

(defn' idx :- IndexSpec
  [index-type index-cols]
  (->IndexSpec index-type index-cols))

(defmacro defindexfn
  "Define an index fn."
  [fn-name index-type]
  `(defn ~fn-name
     [& [arg0# & args#]]
     (cond (map? arg0#)
           (let [index# (idx ~index-type (first args#))]
             (update-in arg0# [:columns+keys] conj index#))
           (or (named? arg0#) (sequential? arg0#))
           (idx ~index-type arg0#))))

(defindexfn primary-key "PRIMARY KEY")
(defindexfn index "INDEX")
(defindexfn unique "UNIQUE")
(defindexfn fulltext "FULLTEXT")
(defindexfn spatial "SPATIAL INDEX")

(defrecord' ForeignKeySpec
  [field :- NamedOrColumn
   references :- (Pair KeywordOrSym KeywordOrSym)
   options :- [KeywordOrSym]]

  AlterAdd
  (-add [spec m]
    (->> (->AddIndexSpec spec)
         (update-in m [:columns+keys] conj)))

  ToSQL
  (to-sql [_]
    (space-join
      (list* (str "FOREIGN KEY"
                  (paren-wrap (parse-name field)))
             "REFERENCES"
             (str (parse-identifier (first references))
                  (-> (second references)
                      parse-identifier
                      paren-wrap))
             (map to-sql options)))))

(defn foreign-key
  "Define a foreign key. Use with create or alt."
  [& [arg0 & args]]
  (cond (map? arg0)
        (let [m arg0
              [field references & options] args
              index (->ForeignKeySpec field references options)]
          (update-in m [:columns+keys] conj index))
        (named? arg0)
        (let [field arg0
              [references & options] args]
          (->ForeignKeySpec field references options))))

(defrecord' ConstraintSpec
  [key-name :- Named
   key-spec :- (U IndexSpec ForeignKeySpec)]

  WhatsMyName
  (whats-my-name [_] key-name)

  AlterAdd
  (-add [spec m]
    (->> (->AddIndexSpec spec)
         (update-in m [:columns+keys] conj)))

  ToSQL
  (to-sql [_]
    (space-join
      (list "CONSTRAINT"
            (parse-identifier key-name)
            (to-sql key-spec)))))

(defn constraint
  "Define a named constraint. Use with an index fn."
  [& [arg0 & args]]
  (cond (map? arg0)
        (let [m arg0
              [key-name key-spec] args
              index (->ConstraintSpec key-name key-spec)]
          (update-in m [:columns+keys] conj index))
        (named? arg0)
        (let [key-name arg0
              [key-spec] args]
          (->ConstraintSpec key-name key-spec))))

(defn' index? :- Boolean
  [x :- Any]
  (or (instance? IndexSpec x)
      (instance? ForeignKeySpec x)
      (instance? ConstraintSpec x)))

(def Index
  "Index type definition."
  (U IndexSpec ForeignKeySpec ConstraintSpec))

(def ColumnOrIndex
  "Column or index type definition."
  (U ColumnSpec Index))

(defrecord' TableSpec
  [table-name :- Named
   temporary :- Boolean
   columns+keys :- [Any]]

  WhatsMyName
  (whats-my-name [_] table-name)

  ToSQL
  (to-sql [_]
    (let [spec (comma-join (map to-sql columns+keys))]
      (str (when temporary "TEMPORARY ")
           "TABLE"
           (pad (parse-identifier table-name))
           (if (= *op* ::create)
             (paren-wrap spec)
             spec)))))

(defn' table :- TableSpec
  [table-name :- Named]
  (->TableSpec table-name false []))

(defn' temp-table :- TableSpec
  [table-name :- Named]
  (->TableSpec table-name true []))

(defn' table? :- Boolean
  [x :- Any]
  (instance? TableSpec x))

(defmacro deftable
  [table-name & columns+keys]
  `(def ~table-name
     (-> (table ~table-name)
         ~@columns+keys)))

(defn' add :- TableSpec
  "Add column/index. Use with alt."
  ([m :- TableSpec, spec]
   (-add spec m))
  ([m :- TableSpec, spec, options]
   (-add spec m options)))

(defrecord' Columns
  [columns+keys :- [ColumnOrIndex]]

  AlterAdd
  (-add [spec m]
    (reduce add m columns+keys)))

(defn' columns :- Columns
  []
  (->Columns []))

(defmacro defcolumns
  "Define one or more columns."
  [name & columns+keys]
  `(def ~name (-> (columns)
                  ~@columns+keys)))

(defn' columns? :- Boolean
  [x :- Any]
  (instance? Columns x))

(defrecord' TableOption
  [option-name :- String
   option-value :- (U Int String)]

  ToSQL
  (to-sql [_]
    (str option-name "=" option-value)))

(defn' engine :- TableOption
  "Set the engine table option."
  [eng :- Named]
  (->TableOption "ENGINE" (name eng)))

(defn' collate :- TableOption
  "Set the collate table option."
  [collation :- Named]
  (->TableOption "COLLATE" (-> (name collation)
                               undasherize)))

(defn' character-set :- TableOption
  "Set the character set table option."
  [charset :- Named]
  (->TableOption "CHARACTER SET" (name charset)))

(defn' auto-inc :- TableOption
  "Set the auto-increment table option."
  [start :- Int]
  (->TableOption "AUTO_INCREMENT" start))

(defrecord' AddIndexSpec
  [index-spec :- Index]

  ToSQL
  (to-sql [_]
    (str "ADD " (to-sql index-spec))))

(defrecord' ChangeSpec
  [col :- NamedOrColumn
   col-spec :- ColumnSpec]

  ToSQL
  (to-sql [_]
    (str "CHANGE "
         (parse-name col) " "
         (to-sql col-spec))))

(defn' change :- TableSpec
  "Change column. Use with alt."
  [m :- TableSpec
   col :- NamedOrColumn
   col-spec :- ColumnSpec]
  (let [stmt (->ChangeSpec col col-spec)]
    (update-in m [:columns+keys] conj stmt)))

(defrecord' SetDefaultSpec
  [col :- NamedOrColumn
   default :- (U String Number)]

  ToSQL
  (to-sql [_]
    (space-join
      (list "ALTER COLUMN"
            (parse-name col)
            "SET DEFAULT"
            default))))

(defn' set-default :- TableSpec
  "Set column default. Use with alt."
  [m :- TableSpec
   col :- NamedOrColumn
   default :- (U String Number)]
  (let [stmt (->SetDefaultSpec col default)]
    (update-in m [:columns+keys] conj stmt)))

(defrecord' DropDefaultSpec
  [col :- NamedOrColumn]

  ToSQL
  (to-sql [_]
    (space-join
      (list "ALTER COLUMN"
            (parse-name col)
            "DROP DEFAULT"))))

(defn' drop-default :- TableSpec
  "Drop column default. Use with alt."
  [m :- TableSpec, col :- NamedOrColumn]
  (let [stmt (->DropDefaultSpec col)]
    (update-in m [:columns+keys] conj stmt)))

(defrecord' DropColumnSpec
  [col :- NamedOrColumn]

  ToSQL
  (to-sql [_]
    (str "DROP COLUMN " (parse-name col))))

(defn' drop-column :- TableSpec
  "Drop a column. Use with alt."
  [m :- TableSpec, col :- NamedOrColumn]
  (let [stmt (->DropColumnSpec col)]
    (update-in m [:columns+keys] conj stmt)))

(def NamedOrConstraint (U Named ConstraintSpec))

(defrecord' DropIndexSpec
  [index :- NamedOrConstraint]

  ToSQL
  (to-sql [_]
    (str "DROP INDEX "
         (parse-name index))))

(defn' drop-index :- TableSpec
  "Drop an index. Use with alt."
  [m :- TableSpec, index :- NamedOrConstraint]
  (let [stmt (->DropIndexSpec index)]
    (update-in m [:columns+keys] conj stmt)))

(defrecord' DropPrimaryKeySpec []
  ToSQL
  (to-sql [_] "DROP PRIMARY KEY"))

(defn' drop-primary-key :- TableSpec
  "Drop a primary key. Use with alt."
  [m :- TableSpec]
  (let [stmt (->DropPrimaryKeySpec)]
    (update-in m [:columns+keys] conj stmt)))

(defrecord' DropForeignKeySpec
  [fk :- NamedOrConstraint]

  ToSQL
  (to-sql [_]
    (str "DROP FOREIGN KEY "
         (parse-name fk))))

(defn' drop-foreign-key :- TableSpec
  "Drop a foreign key. Use with alt."
  [m :- TableSpec, fk :- NamedOrConstraint]
  (let [stmt (->DropForeignKeySpec fk)]
    (update-in m [:columns+keys] conj stmt)))

(def NamedOrTable
  "Named or table spec type definition."
  (U Named TableSpec))

(defrecord' RenameSpec
  [table :- NamedOrTable]

  ToSQL
  (to-sql [_]
    (str "RENAME " (parse-name table))))

(defn' rename :- TableSpec
  "Rename a table. Use with alt."
  [m :- TableSpec, table :- NamedOrTable]
  (let [stmt (->RenameSpec table)]
    (update-in m [:columns+keys] conj stmt)))

(def TableOrDB
  "Table spec or DB spec type definition."
  (U TableSpec DBSpec))

(defn' create :- String
  "Create a table or database with options."
  [spec :- TableOrDB & options :- [TableOption]]
  (binding [*op* ::create]
    (str "CREATE "
         (to-sql spec)
         (when options
           (str " " (comma-join (map to-sql options)))))))

(defn' alt :- String
  "Alter a table."
  [spec :- TableSpec]
  (binding [*op* ::alter]
    (str "ALTER " (to-sql spec))))

(def NamedOrDB
  "Named or DB spec type definition."
  (U Named DBSpec))

(defn' alt-db :- String
  "Alter a database."
  [db :- NamedOrDB, option :- TableOption]
  (str "ALTER DATABASE "
       (parse-name db) " "
       (to-sql option)))

(defn' drop-table :- String
  "Drop one or more tables."
  [& tables :- [NamedOrTable]]
  (str "DROP TABLE "
       (-> (map parse-name tables)
           comma-join)))

(defn' drop-temp-table :- String
  "Drop one or more tables."
  [& tables :- [NamedOrTable]]
  (str "DROP TEMPORARY TABLE "
       (-> (map parse-name tables)
           comma-join)))

(defn' drop-db :- String
  "Drop a database."
  [db :- NamedOrDB]
  (str "DROP DATABASE "
       (parse-name db)))

(defn' rename-db :- String
  "Rename a database."
  [old-db :- NamedOrDB, new-db :- NamedOrDB]
  (str "RENAME DATABASE "
       (parse-name old-db)
       " TO "
       (parse-name new-db)))

(defn' truncate :- String
  "Truncate a table."
  [table :- NamedOrTable]
  (str "TRUNCATE TABLE "
       (parse-name table)))

(defn' append :- TableSpec
  "Append a set of columns/indexes or single
  column/index to a table."
  [m :- TableSpec
   x :- (U Columns ColumnSpec Index)]
  (let [y (cond (columns? x)
                (:columns+keys x)
                (or (column? x) (index? x))
                (list x))]
    (update-in m [:columns+keys] into y)))











