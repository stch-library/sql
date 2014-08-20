# stch.sql

A DSL in Clojure for SQL query, DML, and DDL. Supports a majority of MySQL's statements.

Based on code from [Honey SQL](https://github.com/jkk/honeysql) and ideas from [Lobos](http://budu.github.io/lobos/) and [SQLingvo](https://github.com/r0man/sqlingvo).  Many thanks to the authors of those libraries.

## Installation

Add the following to your project dependencies:

```clojure
[stch-library/sql "0.1.1"]
```

## API Documentation

http://stch-library.github.io/sql

Note: This library uses [stch.schema](https://github.com/stch-library/schema). Please refer to that project page for more information regarding type annotations and their meaning.

## How to use

This library is split into two distinct offerings.

1. Query and DML
2. DDL

Query and DML functions are in stch.sql, accompanied by stch.sql.format.  DDL functions are in stch.sql.ddl.

Below is a small sampling of what you can do with this library.  Please see the unit-tests in the spec directory for a comprehensive look at what's possible.

### Query

Queries are composed of threaded fn calls, producing a map, which when formatted will produce a JDBC compatible vector.

```clojure
(require '[stch.sql.format :as sql]
         '[stch.sql :refer :all])

(-> (select :*)
    (from :foo)
    (where '(= firstName "Billy"))
    sql/format)
; ["SELECT * FROM foo WHERE firstName = ?" "Billy"]
```

If you are using [clojure.jdbc](https://github.com/niwibe/clojure.jdbc), you can extend the ISQLStatement protocol, so that you don't have to manually call sql/format.

```clojure
(require '[jdbc.types :as types])

(extend-protocol types/ISQLStatement
  clojure.lang.APersistentMap
  (normalize [this conn options]
    (types/normalize (sql/format this) conn options)))

(let [id 1234
      sql (-> (select :*)
              (from :users)
              (where `(= userID ~id)))
      result (query conn sql {:identifiers identity})]
  (-> result first))
```

SELECT

```clojure
(-> (select :*)
    sql/format)
; ["SELECT *"]

; Dashes to underscores
(-> (select :first-name)
    sql/format)
; ["SELECT first_name"]

; NULL
(-> (select nil)
    sql/format)
; ["SELECT NULL"]

; Aliased table name
(-> (select [:firstName :name])
    sql/format)
; ["SELECT firstName AS name"]

; Modifiers
(-> (select :id)
    (modifiers :distinct)
    (from :users)
    sql/format)
; ["SELECT DISTINCT id FROM users"]
```

Functions

```clojure
; Prefix
(-> (select '(now))
    sql/format)
; ["SELECT now()"]

; Infix
(-> (select '(<> 1 2)) sql/format)
; ["SELECT 1 <> 2"]

; Convenience shorthand
(-> (select '(count-distinct id))
    sql/format)
; ["SELECT COUNT(DISTINCT id)"]
```

WHERE clause

```clojure
; Implicit AND
(-> (select :*)
    (from :users)
    (where '(= id 5)
           '(= status "active"))
    sql/format)
; ["SELECT * FROM users WHERE (id = 5 AND status = ?)" "active"]

; OR
(-> (select :*)
    (from :users)
    (where '(or (= id 5)
                (= status "active")))
    sql/format)
; ["SELECT * FROM users WHERE (id = 5 OR status = ?)" "active"]
```

Parameters

```clojure
; Named
(-> (select :*)
    (from :users)
    (where '(= name ?name)
           '(= userID ?userID))
    (sql/format :params {:name "Billy"
                         :userID 3}))
; ["SELECT * FROM users WHERE (name = ? AND userID = ?)" "Billy" 3]

; Sequential
(-> (select :*)
    (from :users)
    (where '(= name ?name)
           '(= userID ?userID))
    (sql/format :params ["Billy" 3]))
; ["SELECT * FROM users WHERE (name = ? AND userID = ?)" "Billy" 3]

; Unnamed
(-> (select :*)
    (from :users)
    (where '(= name ?)
           '(= userID ?))
    (sql/format :params ["Billy" 3]))
; ["SELECT * FROM users WHERE (name = ? AND userID = ?)" "Billy" 3]

; Spliced
(def n "Billy")

(-> (select :*)
    (from :foo)
    (where `(= name ~n))
    sql/format)
; ["SELECT * FROM foo WHERE name = ?" "Billy"]
```

JOIN: join, left-join, right-join

```clojure
(-> (select :*)
    (from :users)
    (join :contacts
          '(= users.id contacts.id))
    sql/format)
; ["SELECT * FROM users INNER JOIN contacts ON users.id = contacts.id"]

; Aliased table name
(-> (select :*)
    (from :foo)
    (join :bar '(= foo.x bar.x)
          [:baz :b] '(= bar.x b.x))
    sql/format)
; ["SELECT * FROM foo INNER JOIN bar ON foo.x = bar.x INNER JOIN baz AS b ON bar.x = b.x"]
```

ORDER BY

```clojure
(-> (select :*)
    (from :users)
    (order-by :name :age)
    sql/format)
; ["SELECT * FROM users ORDER BY name, age"]

; DESC
(-> (select :*)
    (from :users)
    (order-by (desc :name))
    sql/format)
; ["SELECT * FROM users ORDER BY name DESC"]
```

GROUP BY

```clojure
(-> (select :*)
    (from :users)
    (group :name :age)
    sql/format)
; ["SELECT * FROM users GROUP BY name, age"]
```

HAVING

```clojure
(-> (select :*)
    (from :users)
    (having '(> (count email) 2))
    sql/format)
; ["SELECT * FROM users HAVING count(email) > 2"]
```

LIMIT

```clojure
(-> (select :*)
    (from :users)
    (limit 50)
    sql/format)
; ["SELECT * FROM users LIMIT 50"]

; OFFSET
(-> (select :*)
    (from :users)
    (limit 50)
    (offset 50)
    sql/format)
; ["SELECT * FROM users LIMIT 50 OFFSET 50"]
```

Subqueries

```clojure
(-> (select :*)
    (from :users)
    (where `(in id
                ~(-> (select :userid)
                     (from :contacts))))
    sql/format)
; ["SELECT * FROM users WHERE (id IN (SELECT userid FROM contacts))"]
```

UNION

```clojure
(sql/format
  (union (-> (select :name :email)
             (from :users))
         (-> (select :name :email)
             (from :deleted-users))))
; ["(SELECT name, email FROM users) UNION (SELECT name, email FROM deleted_users)"]
```

Order doesn't matter

```clojure
(-> (where '(= userid 1234))
    (from :users)
    (order-by :first-name :last-name)
    (select :first-name :last-name)
    sql/format)
; ["SELECT first_name, last_name FROM users WHERE userid = 1234 ORDER BY first_name, last_name"]
```

Composition

select, from, join, left-join, right-join, where, having, group, and order-by are compositional by design.  The way in which each composes should be fairly intuitive.  Here are some examples.

```clojure
(def query
  (-> (select :first-name)
      (from :users)
      (join :contacts '(= users.cid contacts.cid))
      (where '(= id 5))
      (group :first-name)
      (order-by :first-name)))

(-> query
    (select :last-name)
    (join :perms '(= users.uid perms.uid))
    (where '(= status "active"))
    (group :last-name)
    (order-by :last-name)
    sql/format)
; ["SELECT first_name, last_name FROM users INNER JOIN contacts ON users.cid = contacts.cid INNER JOIN perms ON users.uid = perms.uid WHERE (id = 5 AND status = ?) GROUP BY first_name, last_name ORDER BY first_name, last_name" "active"]
```

### DML

INSERT

```clojure
; Vector of maps
(-> (insert-into :users)
    (values [{:name "Billy" :age 35}
             {:name "Joey" :age 37}])
    sql/format)
; ["INSERT INTO users (age, name) VALUES (35, ?), (37, ?)" "Billy" "Joey"]

; Vector of vectors
(-> (insert-into :users)
    (values [["Billy" 35]["Joey" 37]])
    sql/format)
; ["INSERT INTO users VALUES (?, 35), (?, 37)" "Billy" "Joey"]

; ON DUPLICATE KEY
(-> (insert-into :foo)
    (columns :a :b :c)
    (values [[1 2 3]])
    (on-dup-key {:c 9})
    sql/format)
; ["INSERT INTO foo (a, b, c) VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE c = 9"]
```

INSERT/SELECT

```clojure
(-> (insert-into :foo)
    (columns :a :b :c)
    (select :x.a :y.b :z.c)
    (from :x)
    (join :y '(= x.id y.id)
          :z '(= y.id z.id))
    sql/format)
; ["INSERT INTO foo (a, b, c) SELECT x.a, y.b, z.c FROM x INNER JOIN y ON x.id = y.id INNER JOIN z ON y.id = z.id"]
```

UPDATE

```clojure
(-> (update :users)
    (setv {:name "Billy", :age 35})
    (where '(= id 234))
    sql/format)
; ["UPDATE users SET age = 35, name = ? WHERE id = 234" "Billy"]
```

DELETE

```clojure
(-> (delete-from :foo)
    (where '(= email "billy@bob.com"))
    sql/format)
; ["DELETE FROM foo WHERE email = ?" "billy@bob.com"]

; Multiple tables
(-> (delete-from :t1)
    (using :t1 :t2)
    (where '(= t1.x t2.x)
           '(= t2.y 3))
    sql/format)
; ["DELETE FROM t1 USING t1, t2 WHERE (t1.x = t2.x AND t2.y = 3)"]
```

REPLACE (same behavior as insert)

```clojure
(-> (replace-into :users)
    (values [{:name "Billy" :age 35}
             {:name "Joey" :age 37}])
    sql/format)
; ["REPLACE INTO users (age, name) VALUES (35, ?), (37, ?)" "Billy" "Joey"]
```

### Quoting

Quote style to use for identifiers. Options include:

1. :ansi (PostgreSQL)
2. :mysql
3. :sqlserver
4. :oracle

Defaults to no quoting.

```clojure
(-> (select :users.name
            :contacts.*
            '(date_format dob "%m/%d/%Y"))
    (from :users)
    (join :contacts
          '(= users.id contacts.userid))
    (where '(in users.status ["active"
                              "pending"]))
    (group :users.status)
    (order-by (asc :contacts.last-name))
    (limit 25)
    (sql/format :quoting :mysql))
; ["SELECT `users`.`name`, `contacts`.*, date_format(`dob`, ?) FROM `users` INNER JOIN `contacts` ON `users`.`id` = `contacts`.`userid` WHERE (`users`.`status` IN (?, ?)) GROUP BY `users`.`status` ORDER BY `contacts`.`last_name` ASC LIMIT 25" "%m/%d/%Y" "active" "pending"]
```

### DDL

The two primary functions are create and alt, which take a table record and produce a SQL string.

#### CREATE

```clojure
(use 'stch.sql.ddl)

(create
  (-> (table :users)
      (integer :userID :unsigned :not-null)
      (integer :orgID)
      (set' :groups ["user" "admin"] (default "user"))
      (enum :status ["active" "inactive"])
      (decimal :ranking '(3 1) (default 0))
      (varchar :username [50])
      (chr :countryCode [2] (default "US"))
      (primary-key :userID)
      (index [:userID :orgID])
      (unique :username)
      (foreign-key :orgID '(orgs orgID) :on-delete-cascade))
 (engine :InnoDB)
 (collate :utf8-general-ci))
; "CREATE TABLE users (userID INT UNSIGNED NOT NULL, orgID INT, groups SET('user', 'admin') DEFAULT 'user', status ENUM('active', 'inactive'), ranking DECIMAL(3, 1) DEFAULT 0, username VARCHAR(50), countryCode CHAR(2) DEFAULT 'US', PRIMARY KEY(userID), INDEX(userID, orgID), UNIQUE(username), FOREIGN KEY(orgID) REFERENCES orgs(orgID) ON DELETE CASCADE) ENGINE=InnoDB, COLLATE=utf8_general_ci"
```

All column types have a corresponding function.  See API for details.

```clojure
(create
  (-> (table :users)
      (chr :countryCode [2])))
; "CREATE TABLE users (countryCode CHAR(2))"
```

INDEX

```clojure
(create
  (-> (table :users)
      (integer :user_id)
      (index :user_id)))
; "CREATE TABLE users (user_id INT, INDEX(user_id))"

; Multiple columns
(create
  (-> (table :users)
      (varchar :first_name [100])
      (varchar :last_name [100])
      (index [:first_name :last_name])))
; "CREATE TABLE users (first_name VARCHAR(100), last_name VARCHAR(100), INDEX(first_name, last_name))"

; FOREIGN KEY
(create
  (-> (table :contacts)
      (integer :user_id)
      (foreign-key :user_id '(users user_id) :on-delete-cascade)))
; "CREATE TABLE contacts (user_id INT, FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE)"

; Named key
(create
  (-> (table :users)
      (varchar :username [50])
      (constraint :uname (unique :username))))
; "CREATE TABLE users (username VARCHAR(50), CONSTRAINT uname UNIQUE(username))"
```

Table Options

```clojure
(create
   (-> (table :users)
       (varchar :username [50]))
  (engine :InnoDB)
  (character-set :utf8))
; "CREATE TABLE users (username VARCHAR(50)) ENGINE=InnoDB, CHARACTER SET=utf8"
```

Appending

```clojure
(defcolumns cols
  (-> (integer :user-id :unsigned :not-null)
      (index :user-id)))

(create
  (-> (table :users)
      (append cols)
      (varchar :username [50])))
; "CREATE TABLE users (user_id INT UNSIGNED NOT NULL, INDEX(user_id), username VARCHAR(50))"
```

TEMPORARY TABLE

```clojure
(create
  (-> (temp-table :users)
      (integer :userID)))
; "CREATE TEMPORARY TABLE users (userID INT)"
```

#### ALTER

```clojure
(alt
  (-> (table :users)
      (add (varchar :email [50]) (after :userID))
      (add (varchar :firstName [25]) :first)
      (add (index [:firstName :lastName]))
      (add (index '(username ranking)))
      (add (foreign-key :orgID '(orgs orgID) :on-delete-cascade))
      (change :username (varchar :username [100]))
      (drop-default :ranking)
      (set-default :ranking 1)
      (drop-column :countryCode)
      (drop-index :uname)
      (drop-primary-key)
      (drop-foreign-key :fk1)))
; "ALTER TABLE users ADD COLUMN email VARCHAR(50) AFTER userID, ADD COLUMN firstName VARCHAR(25) FIRST, ADD INDEX(firstName, lastName), ADD INDEX(username, ranking), ADD FOREIGN KEY(orgID) REFERENCES orgs(orgID) ON DELETE CASCADE, CHANGE username username VARCHAR(100), ALTER COLUMN ranking DROP DEFAULT, ALTER COLUMN ranking SET DEFAULT 1, DROP COLUMN countryCode, DROP INDEX uname, DROP PRIMARY KEY, DROP FOREIGN KEY fk1"
```

ADD

```clojure
; AFTER
(alt
  (-> (table :users)
      (add (varchar :email [50]) (after :userID))))
; "ALTER TABLE users ADD COLUMN email VARCHAR(50) AFTER userID"

; FIRST
(alt
  (-> (table :users)
      (add (varchar :email [50]) :first)))
; "ALTER TABLE users ADD COLUMN email VARCHAR(50) FIRST"

; INDEX
(alt
  (-> (table :users)
      (add (index :firstName))))
; "ALTER TABLE users ADD INDEX(firstName)"
```

CHANGE

```clojure
(alt
  (-> (table :users)
      (change :username (varchar :username [100]))))
; "ALTER TABLE users CHANGE username username VARCHAR(100)"
```

SET DEFAULT

```clojure
(alt
  (-> (table :users)
      (set-default :ranking 1)))
; "ALTER TABLE users ALTER COLUMN ranking SET DEFAULT 1"
```

DROP COLUMN

```clojure
(alt
  (-> (table :users)
      (drop-column :username)))
; "ALTER TABLE users DROP COLUMN username"
```

## Unit-tests

Run "lein spec"
