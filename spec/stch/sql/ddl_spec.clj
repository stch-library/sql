(ns stch.sql.ddl-spec
  (:require
   ;; exclude speclj.core/update
   [speclj.core :refer [describe around context it should=]]
   [stch.schema :refer [with-fn-validation]]
   [stch.sql.ddl :refer :all]
   [stch.util :refer [named?]]))

(describe "create table"
  (around [it]
    (with-fn-validation (it)))
  (context "columns"
    (context "numeric"
      (it "bool"
        (should= "CREATE TABLE users (enabled BOOL)"
                 (create
                   (-> (table :users)
                       (bool :enabled)))))
      (it "tiny-int"
        (should= "CREATE TABLE users (age TINYINT)"
                 (create
                   (-> (table :users)
                       (tiny-int :age)))))
      (it "small-int"
        (should= "CREATE TABLE users (points SMALLINT)"
                 (create
                   (-> (table :users)
                       (small-int :points)))))
      (it "medium-int"
        (should= "CREATE TABLE users (points MEDIUMINT)"
                 (create
                   (-> (table :users)
                       (medium-int :points)))))
      (it "integer"
        (should= "CREATE TABLE users (points INT)"
                 (create
                   (-> (table :users)
                       (integer :points)))))
      (it "big-int"
        (should= "CREATE TABLE users (points BIGINT)"
                 (create
                   (-> (table :users)
                       (big-int :points)))))
      (it "decimal"
        (should= "CREATE TABLE users (score DECIMAL(3, 1))"
                 (create
                   (-> (table :users)
                       (decimal :score [3 1])))))
      (it "float'"
        (should= "CREATE TABLE users (points FLOAT)"
                 (create
                   (-> (table :users)
                       (float' :points)))))
      (it "double'"
        (should= "CREATE TABLE users (points DOUBLE)"
                 (create
                   (-> (table :users)
                       (double' :points))))))
    (context "serial"
      (it "small-serial"
        (should= "CREATE TABLE users (id SMALLSERIAL)"
                 (create
                   (-> (table :users)
                       (small-serial :id)))))
      (it "serial"
        (should= "CREATE TABLE users (id SERIAL)"
                 (create
                   (-> (table :users)
                       (serial :id)))))
      (it "big-serial"
        (should= "CREATE TABLE users (id BIGSERIAL)"
                 (create
                   (-> (table :users)
                       (big-serial :id))))))
    (context "string"
      (it "chr"
        (should= "CREATE TABLE users (countryCode CHAR(2))"
                 (create
                   (-> (table :users)
                       (chr :countryCode [2])))))
      (it "varchar"
        (should= "CREATE TABLE users (name VARCHAR(50))"
                 (create
                   (-> (table :users)
                       (varchar :name [50])))))
      (it "binary"
        (should= "CREATE TABLE users (countryCode BINARY(2))"
                 (create
                   (-> (table :users)
                       (binary :countryCode [2])))))
      (it "varbinary"
        (should= "CREATE TABLE users (name VARBINARY(50))"
                 (create
                   (-> (table :users)
                       (varbinary :name [50])))))
      (it "blob"
        (should= "CREATE TABLE users (aboutMe BLOB)"
                 (create
                   (-> (table :users)
                       (blob :aboutMe)))))
      (it "text"
        (should= "CREATE TABLE users (aboutMe TEXT)"
                 (create
                   (-> (table :users)
                       (text :aboutMe)))))
      (it "enum"
        (should= "CREATE TABLE users (status ENUM('active', 'inactive'))"
                 (create
                   (-> (table :users)
                       (enum :status ["active" "inactive"])))))
      (it "set'"
        (should= "CREATE TABLE users (groups SET('user', 'admin'))"
                 (create
                   (-> (table :users)
                       (set' :groups ["user" "admin"]))))))
    (context "date and time"
      (it "date"
        (should= "CREATE TABLE users (activated DATE)"
                 (create
                   (-> (table :users)
                       (date :activated)))))
      (it "datetime"
        (should= "CREATE TABLE users (activated DATETIME)"
                 (create
                   (-> (table :users)
                       (datetime :activated)))))
      (it "time'"
        (should= "CREATE TABLE logs (instant TIME)"
                 (create
                   (-> (table :logs)
                       (time' :instant)))))
      (it "year"
        (should= "CREATE TABLE users (birthYear YEAR)"
                 (create
                   (-> (table :users)
                       (year :birthYear))))))
    (context "spatial"
      (it "geometry"
        (should= "CREATE TABLE geom (g GEOMETRY)"
                 (create
                   (-> (table :geom)
                       (geometry :g)))))
      (it "point"
        (should= "CREATE TABLE geom (p POINT)"
                 (create
                   (-> (table :geom)
                       (point :p)))))
      (it "linestring"
        (should= "CREATE TABLE geom (l LINESTRING)"
                 (create
                   (-> (table :geom)
                       (linestring :l)))))
      (it "polygon"
        (should= "CREATE TABLE geom (p POLYGON)"
                 (create
                   (-> (table :geom)
                       (polygon :p))))))
    (context "options"
      (it "default"
        (should= "CREATE TABLE users (countryCode CHAR(2) DEFAULT 'US')"
                 (create
                   (-> (table :users)
                       (chr :countryCode [2] (default "US"))))))
      (it "keywords"
        (should= "CREATE TABLE users (user_id INT UNSIGNED NOT NULL PRIMARY KEY)"
                 (create
                   (-> (table :users)
                       (integer :user_id :unsigned :not-null :primary-key)))))
      (it "auto increment"
        (should= "CREATE TABLE users (user_id INT AUTO_INCREMENT)"
                 (create
                   (-> (table :users)
                       (integer :user_id :auto-increment)))))))
  (context "indexes"
    (it "primary-key"
      (should= "CREATE TABLE users (user_id INT, PRIMARY KEY(user_id))"
               (create
                 (-> (table :users)
                     (integer :user_id)
                     (primary-key :user_id)))))
    (context "index"
      (it "single column"
        (should= "CREATE TABLE users (user_id INT, INDEX(user_id))"
                 (create
                   (-> (table :users)
                       (integer :user_id)
                       (index :user_id)))))
      (it "multi-column"
        (should= "CREATE TABLE users (first_name VARCHAR(100), last_name VARCHAR(100), INDEX(first_name, last_name))"
                 (create
                   (-> (table :users)
                       (varchar :first_name [100])
                       (varchar :last_name [100])
                       (index [:first_name :last_name])))))
      (let [first-name (varchar :first_name [100])]
        (it "column record"
          (should= "CREATE TABLE users (first_name VARCHAR(100), INDEX(first_name))"
                   (create
                     (-> (table :users)
                         (append first-name)
                         (index first-name)))))))
    (it "unique"
      (should= "CREATE TABLE users (user_id INT, UNIQUE(user_id))"
               (create
                 (-> (table :users)
                     (integer :user_id)
                     (unique :user_id)))))
    (it "foreign key"
      (should= "CREATE TABLE contacts (user_id INT, FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE)"
               (create
                 (-> (table :contacts)
                     (integer :user_id)
                     (foreign-key :user_id '(users user_id) :on-delete-cascade)))))
    (it "fulltext"
      (should= "CREATE TABLE users (name VARCHAR(50), FULLTEXT(name))"
               (create
                 (-> (table :users)
                     (varchar :name [50])
                     (fulltext :name)))))
    (it "spatial"
      (should= "CREATE TABLE geom (g GEOMETRY, SPATIAL INDEX(g))"
               (create
                 (-> (table :geom)
                     (geometry :g)
                     (spatial :g)))))
    (it "constraint (named keys)"
      (should= "CREATE TABLE users (username VARCHAR(50), CONSTRAINT uname UNIQUE(username))"
               (create
                 (-> (table :users)
                     (varchar :username [50])
                     (constraint :uname (unique :username)))))))
  (context "table options"
    (it "engine"
      (should= "CREATE TABLE users (username VARCHAR(50)) ENGINE=InnoDB"
               (create
                 (-> (table :users)
                     (varchar :username [50]))
                 (engine :InnoDB))))
    (it "collate"
      (should= "CREATE TABLE users (username VARCHAR(50)) COLLATE=utf8_general_ci"
               (create
                 (-> (table :users)
                     (varchar :username [50]))
                 (collate :utf8-general-ci))))
    (it "character-set"
      (should= "CREATE TABLE users (username VARCHAR(50)) CHARACTER SET=utf8"
               (create
                 (-> (table :users)
                     (varchar :username [50]))
                 (character-set :utf8))))
    (it "auto-inc"
      (should= "CREATE TABLE users (username VARCHAR(50)) AUTO_INCREMENT=1000"
               (create
                 (-> (table :users)
                     (varchar :username [50]))
                 (auto-inc 1000)))))
  (context "append"
    (let [cols (-> (columns)
                   (integer :user-id :unsigned :not-null)
                   (index :user-id))]
      (it "columns"
        (should= "CREATE TABLE users (user_id INT UNSIGNED NOT NULL, INDEX(user_id), username VARCHAR(50))"
                 (create
                   (-> (table :users)
                       (append cols)
                       (varchar :username [50]))))))
    (let [col (integer :user-id :unsigned :not-null)]
      (it "column"
        (should= "CREATE TABLE users (user_id INT UNSIGNED NOT NULL, username VARCHAR(50))"
                 (create
                   (-> (table :users)
                       (append col)
                       (varchar :username [50]))))))
    (let [idx (index :user-id)]
      (it "index"
        (should= "CREATE TABLE users (user_id INT UNSIGNED NOT NULL, INDEX(user_id))"
                 (create
                   (-> (table :users)
                       (integer :user-id :unsigned :not-null)
                       (append idx)))))))
  (it "temporary"
    (should= "CREATE TEMPORARY TABLE users (userID INT)"
             (create
               (-> (temp-table :users)
                   (integer :userID)))))
  (it "complex"
    (should= "CREATE TABLE users (userID INT UNSIGNED NOT NULL, orgID INT, groups SET('user', 'admin') DEFAULT 'user', status ENUM('active', 'inactive'), ranking DECIMAL(3, 1) DEFAULT 0, username VARCHAR(50), countryCode CHAR(2) DEFAULT 'US', PRIMARY KEY(userID), INDEX(userID, orgID), UNIQUE(username), FOREIGN KEY(orgID) REFERENCES orgs(orgID) ON DELETE CASCADE) ENGINE=InnoDB, COLLATE=utf8_general_ci"
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
               (collate :utf8-general-ci)))))

(describe "alter table"
  (around [it]
    (with-fn-validation (it)))
  (context "add column"
    (it "no position"
      (should= "ALTER TABLE users ADD COLUMN email VARCHAR(50)"
               (alt
                 (-> (table :users)
                     (add (varchar :email [50]))))))
    (context "after"
      (it "keyword"
        (should= "ALTER TABLE users ADD COLUMN email VARCHAR(50) AFTER userID"
                 (alt
                   (-> (table :users)
                       (add (varchar :email [50]) (after :userID))))))
      (it "column record"
        (should= "ALTER TABLE users ADD COLUMN email VARCHAR(50) AFTER userID"
                 (alt
                   (-> (table :users)
                       (add (varchar :email [50])
                            (after (integer :userID))))))))
    (it "first"
      (should= "ALTER TABLE users ADD COLUMN email VARCHAR(50) FIRST"
               (alt
                 (-> (table :users)
                     (add (varchar :email [50]) :first)))))
    (let [cols (-> (columns)
                   (varchar :firstName [50])
                   (varchar :lastName [50]))]
      (it "columns"
        (should= "ALTER TABLE users ADD COLUMN firstName VARCHAR(50), ADD COLUMN lastName VARCHAR(50)"
                 (alt
                   (-> (table :users)
                       (add cols)))))))
  (context "add index"
    (it "index"
      (should= "ALTER TABLE users ADD INDEX(firstName)"
               (alt
                 (-> (table :users)
                     (add (index :firstName))))))
    (it "foreign key"
      (should= "ALTER TABLE users ADD FOREIGN KEY(orgID) REFERENCES orgs(orgID) ON DELETE CASCADE"
               (alt
                 (-> (table :users)
                     (add (foreign-key :orgID '(orgs orgID) :on-delete-cascade)))))))
  (context "change"
    (it "keyword"
      (should= "ALTER TABLE users CHANGE username username VARCHAR(100)"
               (alt
                 (-> (table :users)
                     (change :username (varchar :username [100]))))))
    (let [username (varchar :username [50])]
      (it "column record"
        (should= "ALTER TABLE users CHANGE username username VARCHAR(100)"
                 (alt
                   (-> (table :users)
                       (change username (varchar :username [100]))))))))
  (context "set-default"
    (it "keyword"
      (should= "ALTER TABLE users ALTER COLUMN ranking SET DEFAULT 1"
               (alt
                 (-> (table :users)
                     (set-default :ranking 1)))))
    (let [ranking (integer :ranking)]
      (it "column record"
        (should= "ALTER TABLE users ALTER COLUMN ranking SET DEFAULT 1"
                 (alt
                   (-> (table :users)
                       (set-default ranking 1)))))))
  (context "drop-default"
    (it "keyword"
      (should= "ALTER TABLE users ALTER COLUMN ranking DROP DEFAULT"
               (alt
                 (-> (table :users)
                     (drop-default :ranking)))))
    (let [ranking (integer :ranking)]
      (it "column record"
        (should= "ALTER TABLE users ALTER COLUMN ranking DROP DEFAULT"
                 (alt
                   (-> (table :users)
                       (drop-default ranking)))))))
  (context "drop-column"
    (it "keyword"
      (should= "ALTER TABLE users DROP COLUMN username"
               (alt
                 (-> (table :users)
                     (drop-column :username)))))
    (let [username (varchar :username [25])]
      (it "column record"
        (should= "ALTER TABLE users DROP COLUMN username"
                 (alt
                   (-> (table :users)
                       (drop-column username)))))))
  (context "drop-index"
    (it "keyword"
      (should= "ALTER TABLE users DROP INDEX uname"
               (alt
                 (-> (table :users)
                     (drop-index :uname)))))
    (let [idx (constraint :uname (index :username))]
      (it "constraint record"
        (should= "ALTER TABLE users DROP INDEX uname"
                 (alt
                   (-> (table :users)
                       (drop-index idx)))))))
  (it "drop-primary-key"
    (should= "ALTER TABLE users DROP PRIMARY KEY"
             (alt
               (-> (table :users)
                   (drop-primary-key)))))
  (context "drop-foreign-key"
    (it "keyword"
      (should= "ALTER TABLE users DROP FOREIGN KEY fk1"
               (alt
                 (-> (table :users)
                     (drop-foreign-key :fk1)))))
    (let [idx (constraint :fk1 (foreign-key :orgID '(orgs orgID) :on-delete-cascade))]
      (it "constraint record"
        (should= "ALTER TABLE users DROP FOREIGN KEY fk1"
                 (alt
                   (-> (table :users)
                       (drop-foreign-key idx)))))))
  (context "rename table"
    (it "keyword"
      (should= "ALTER TABLE system_users RENAME users"
               (alt (-> (table :system-users)
                        (rename :users)))))
    (let [tbl (table :users)]
      (it "table record"
        (should= "ALTER TABLE system_users RENAME users"
                 (alt (-> (table :system-users)
                          (rename tbl)))))))
  (it "complex"
    (should= "ALTER TABLE users ADD COLUMN email VARCHAR(50) AFTER userID, ADD COLUMN firstName VARCHAR(25) FIRST, ADD INDEX(firstName, lastName), ADD INDEX(username, ranking), ADD FOREIGN KEY(orgID) REFERENCES orgs(orgID) ON DELETE CASCADE, CHANGE username username VARCHAR(100), ALTER COLUMN ranking DROP DEFAULT, ALTER COLUMN ranking SET DEFAULT 1, DROP COLUMN countryCode, DROP INDEX uname, DROP PRIMARY KEY, DROP FOREIGN KEY fk1"
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
                   (drop-foreign-key :fk1))))))

(describe "drop table"
  (around [it]
    (with-fn-validation (it)))
  (context "drop-table"
    (it "keyword"
      (should= "DROP TABLE users, contacts"
               (drop-table :users :contacts)))
    (it "table record"
      (should= "DROP TABLE users, contacts"
               (drop-table (table :users)
                           (table :contacts)))))
  (context "drop-temp-table"
    (it "keyword"
      (should= "DROP TEMPORARY TABLE users, contacts"
               (drop-temp-table :users :contacts)))
    (it "table record"
      (should= "DROP TEMPORARY TABLE users, contacts"
               (drop-temp-table (table :users)
                                (table :contacts))))))

(describe "truncate table"
  (around [it]
    (with-fn-validation (it)))
  (it "keyword"
    (should= "TRUNCATE TABLE users"
             (truncate :users)))
  (it "table record"
    (should= "TRUNCATE TABLE users"
             (truncate (table :users)))))

(describe "db"
  (around [it]
    (with-fn-validation (it)))
  (context "create"
    (it "no options"
      (should= "CREATE DATABASE prod"
               (create (db :prod))))
    (it "with options"
      (should= "CREATE DATABASE prod CHARACTER SET=utf8"
               (create (db :prod) (character-set :utf8)))))
  (context "alt-db"
    (it "keyword"
      (should= "ALTER DATABASE prod CHARACTER SET=utf8"
               (alt-db :prod (character-set :utf8))))
    (it "db record"
      (should= "ALTER DATABASE prod CHARACTER SET=utf8"
               (alt-db (db :prod) (character-set :utf8)))))
  (context "drop-db"
    (it "keyword"
      (should= "DROP DATABASE prod"
               (drop-db :prod)))
    (it "db record"
      (should= "DROP DATABASE prod"
               (drop-db (db :prod)))))
  (context "rename-db"
    (it "keyword"
      (should= "RENAME DATABASE prod TO production"
               (rename-db :prod :production)))
    (it "db record"
      (should= "RENAME DATABASE prod TO production"
               (rename-db (db :prod)
                          (db :production))))))
