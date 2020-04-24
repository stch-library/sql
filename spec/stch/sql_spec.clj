(ns stch.sql-spec
  (:require [stch.sql.format :as sql])
  (:use stch.sql speclj.core )
  (:refer-clojure :exclude [update]))

(describe "select"
  (context "select"
    (it "keyword"
      (should= ["SELECT *"]
               (-> (select :*)
                   sql/format)))
    (it "multiple keywords"
      (should= ["SELECT name, email"]
               (-> (select :name :email)
                   sql/format)))
    (it "select x2"
      (should= ["SELECT name, email"]
               (-> (select :name)
                   (select :email)
                   sql/format)))
    (it "dashes to underscores"
      (should= ["SELECT first_name"]
               (-> (select :first-name)
                   sql/format)))
    (it "table.field"
      (should= ["SELECT users.name"]
               (-> (select :users.name)
                   sql/format)))
    (it "symbol"
      (should= ["SELECT name"]
               (-> (select 'name)
                   sql/format)))
    (it "string"
      (should= ["SELECT ?" "name"]
               (-> (select "name")
                   sql/format)))
    (it "nil"
      (should= ["SELECT NULL"]
               (-> (select nil)
                   sql/format)))
    (it "as"
      (should= ["SELECT firstName AS name"]
               (-> (select [:firstName :name])
                   sql/format)))
    (it "prefix function"
      (should= ["SELECT now()"]
               (-> (select '(now))
                   sql/format)))
    (it "infix function"
      (should= ["SELECT 1 <> 2"]
               (-> (select '(<> 1 2))
                   sql/format)))
    (it "nested functions"
      (should= ["SELECT concat(if(gender = ?, ?, ?), lastName)" "M" "Mr." "Ms."]
               (-> (select
                     '(concat
                        (if (= gender "M")
                          "Mr." "Ms.")
                       lastName))
                   sql/format))))
  (context "from"
    (it "keyword"
      (should= ["SELECT * FROM users"]
               (-> (select :*)
                   (from :users)
                   sql/format)))
    (it "keywords"
      (should= ["SELECT * FROM users, contacts"]
               (-> (select :*)
                   (from :users :contacts)
                   sql/format)))
    (it "from x2"
      (should= ["SELECT * FROM users, contacts"]
               (-> (select :*)
                   (from :users)
                   (from :contacts)
                   sql/format)))
    (it "symbol"
      (should= ["SELECT * FROM users"]
               (-> (select :*)
                   (from 'users)
                   sql/format)))
    (it "as"
      (should= ["SELECT * FROM organizations AS orgs"]
               (-> (select :*)
                   (from [:organizations :orgs])
                   sql/format))))
  (context "where"
    (it "list"
      (should= ["SELECT * FROM users WHERE id = 5"]
               (-> (select :*)
                   (from :users)
                   (where '(= id 5))
                   sql/format)))
    (it "quoted list"
      (should= ["SELECT * FROM users WHERE name = ?" "Billy"]
               (let [name "Billy"]
                 (-> (select :*)
                     (from :users)
                     (where `(= name ~name))
                     sql/format))))
    (it "two lists (implicit AND)"
      (should= ["SELECT * FROM users WHERE (id = 5 AND status = ?)" "active"]
               (-> (select :*)
                   (from :users)
                   (where '(= id 5)
                          '(= status "active"))
                   sql/format)))
    (it "AND"
      (should= ["SELECT * FROM users WHERE (id = 5 AND status = ?)" "active"]
               (-> (select :*)
                   (from :users)
                   (where '(and (= id 5)
                                (= status "active")))
                   sql/format)))
    (it "OR"
      (should= ["SELECT * FROM users WHERE (id = 5 OR status = ?)" "active"]
               (-> (select :*)
                   (from :users)
                   (where '(or (= id 5)
                               (= status "active")))
                   sql/format)))
    (it "where x2"
      (should= ["SELECT * FROM users WHERE (id = 5 AND status = ?)" "active"]
               (-> (select :*)
                   (from :users)
                   (where '(= id 5))
                   (where '(= status "active"))
                   sql/format)))
    (it "function call in list"
      (should= ["SELECT * FROM users WHERE activated = curdate()"]
               (-> (select :*)
                   (from :users)
                   (where '(= activated (curdate)))
                   sql/format)))
    (it "IS"
      (should= ["SELECT * FROM users WHERE deactivateDate IS NULL"]
               (-> (select :*)
                   (from :users)
                   (where '(is deactivateDate nil))
                   sql/format)))
    (it "IS NOT"
      (should= ["SELECT * FROM users WHERE deactivateDate IS NOT NULL"]
               (-> (select :*)
                   (from :users)
                   (where '(is-not deactivateDate nil))
                   sql/format)))
    (context "named parameters"
      (it "map params"
        (should= ["SELECT * FROM users WHERE (name = ? AND userID = ?)" "Billy" 3]
                 (-> (select :*)
                     (from :users)
                     (where '(= name ?name)
                            '(= userID ?userID))
                     (sql/format :params {:name "Billy"
                                          :userID 3}))))
      (it "sequential params"
        (should= ["SELECT * FROM users WHERE (name = ? AND userID = ?)" "Billy" 3]
                 (-> (select :*)
                     (from :users)
                     (where '(= name ?name)
                            '(= userID ?userID))
                     (sql/format :params ["Billy" 3])))))
    (context "unnamed parameters"
      (it "sequential params"
        (should= ["SELECT * FROM users WHERE (name = ? AND userID = ?)" "Billy" 3]
                 (-> (select :*)
                     (from :users)
                     (where '(= name ?)
                            '(= userID ?))
                     (sql/format :params ["Billy" 3]))))))
  (context "join"
    (it "single join"
      (should= ["SELECT * FROM users INNER JOIN contacts ON users.id = contacts.id"]
               (-> (select :*)
                   (from :users)
                   (join :contacts
                         '(= users.id contacts.id))
                   sql/format)))
    (it "multiple join"
      (should= ["SELECT * FROM users INNER JOIN contacts ON users.id = contacts.id INNER JOIN orgs ON users.orgid = orgs.orgid"]
               (-> (select :*)
                   (from :users)
                   (join :contacts '(= users.id contacts.id)
                         :orgs '(= users.orgid orgs.orgid))
                   sql/format)))
    (it "join x2"
      (should= ["SELECT * FROM users INNER JOIN contacts ON users.id = contacts.id INNER JOIN orgs ON users.orgid = orgs.orgid"]
               (-> (select :*)
                   (from :users)
                   (join :contacts '(= users.id contacts.id))
                   (join :orgs '(= users.orgid orgs.orgid))
                   sql/format)))
    (it "AS"
      (should= ["SELECT * FROM users INNER JOIN contacts AS c ON users.id = c.id"]
               (-> (select :*)
                   (from :users)
                   (join [:contacts :c]
                         '(= users.id c.id))
                   sql/format))))
  (context "left join"
    (it "single join"
      (should= ["SELECT * FROM users LEFT JOIN contacts ON users.id = contacts.id"]
               (-> (select :*)
                   (from :users)
                   (left-join :contacts
                              '(= users.id contacts.id))
                   sql/format)))
    (it "multiple join"
      (should= ["SELECT * FROM users LEFT JOIN contacts ON users.id = contacts.id LEFT JOIN orgs ON users.orgid = orgs.orgid"]
               (-> (select :*)
                   (from :users)
                   (left-join :contacts '(= users.id contacts.id)
                              :orgs '(= users.orgid orgs.orgid))
                   sql/format)))
    (it "left join x2"
      (should= ["SELECT * FROM users LEFT JOIN contacts ON users.id = contacts.id LEFT JOIN orgs ON users.orgid = orgs.orgid"]
               (-> (select :*)
                   (from :users)
                   (left-join :contacts '(= users.id contacts.id))
                   (left-join :orgs '(= users.orgid orgs.orgid))
                   sql/format))))
  (context "right join"
    (it "single join"
      (should= ["SELECT * FROM users RIGHT JOIN contacts ON users.id = contacts.id"]
               (-> (select :*)
                   (from :users)
                   (right-join :contacts
                              '(= users.id contacts.id))
                   sql/format)))
    (it "multiple join"
      (should= ["SELECT * FROM users RIGHT JOIN contacts ON users.id = contacts.id RIGHT JOIN orgs ON users.orgid = orgs.orgid"]
               (-> (select :*)
                   (from :users)
                   (right-join :contacts '(= users.id contacts.id)
                              :orgs '(= users.orgid orgs.orgid))
                   sql/format)))
    (it "right join x2"
      (should= ["SELECT * FROM users RIGHT JOIN contacts ON users.id = contacts.id RIGHT JOIN orgs ON users.orgid = orgs.orgid"]
               (-> (select :*)
                   (from :users)
                   (right-join :contacts '(= users.id contacts.id))
                   (right-join :orgs '(= users.orgid orgs.orgid))
                   sql/format))))
  (context "order by"
    (it "keyword"
      (should= ["SELECT * FROM users ORDER BY name"]
               (-> (select :*)
                   (from :users)
                   (order-by :name)
                   sql/format)))
    (it "keywords"
      (should= ["SELECT * FROM users ORDER BY name, age"]
               (-> (select :*)
                   (from :users)
                   (order-by :name :age)
                   sql/format)))
    (it "order by x2"
      (should= ["SELECT * FROM users ORDER BY name, age"]
               (-> (select :*)
                   (from :users)
                   (order-by :name)
                   (order-by :age)
                   sql/format)))
    (it "asc"
      (should= ["SELECT * FROM users ORDER BY name ASC"]
               (-> (select :*)
                   (from :users)
                   (order-by (asc :name))
                   sql/format)))
    (it "desc"
      (should= ["SELECT * FROM users ORDER BY name DESC"]
               (-> (select :*)
                   (from :users)
                   (order-by (desc :name))
                   sql/format)))
    (it "function"
      (should= ["SELECT * FROM users INNER JOIN steps ON users.id = steps.userid ORDER BY sum(totalSteps)"]
               (-> (select :*)
                   (from :users)
                   (join :steps '(= users.id steps.userid))
                   (order-by '(sum totalSteps))
                   sql/format))))
  (context "group by"
    (it "keyword"
      (should= ["SELECT * FROM users GROUP BY name"]
               (-> (select :*)
                   (from :users)
                   (group :name)
                   sql/format)))
    (it "keywords"
      (should= ["SELECT * FROM users GROUP BY name, age"]
               (-> (select :*)
                   (from :users)
                   (group :name :age)
                   sql/format)))
    (it "group by x2"
      (should= ["SELECT * FROM users GROUP BY name, age"]
               (-> (select :*)
                   (from :users)
                   (group :name)
                   (group :age)
                   sql/format))))
  (context "having"
    (it "list"
      (should= ["SELECT * FROM users HAVING count(email) > 2"]
               (-> (select :*)
                   (from :users)
                   (having '(> (count email) 2))
                   sql/format)))
    (it "two lists (implicit AND)"
      (should= ["SELECT * FROM users HAVING (count(this) > 2 AND count(that) > 2)"]
               (-> (select :*)
                   (from :users)
                   (having '(> (count this) 2)
                           '(> (count that) 2))
                   sql/format)))
    (it "having x2"
      (should= ["SELECT * FROM users HAVING (count(this) > 2 AND count(that) > 2)"]
               (-> (select :*)
                   (from :users)
                   (having '(> (count this) 2))
                   (having '(> (count that) 2))
                   sql/format))))
  (context "limit"
    (it "number"
      (should= ["SELECT * FROM users LIMIT 50"]
               (-> (select :*)
                   (from :users)
                   (limit 50)
                   sql/format))))
  (context "offset"
    (it "number"
      (should= ["SELECT * FROM users LIMIT 50 OFFSET 50"]
               (-> (select :*)
                   (from :users)
                   (limit 50)
                   (offset 50)
                   sql/format))))
  (context "modifiers"
    (it "keyword"
      (should= ["SELECT DISTINCT id FROM users"]
               (-> (select :id)
                   (modifiers :distinct)
                   (from :users)
                   sql/format))))
  (context "subqueries"
    (it "where clause"
      (should= ["SELECT * FROM users WHERE (id IN (SELECT userid FROM contacts))"]
               (-> (select :*)
                   (from :users)
                   (where `(in id
                               ~(-> (select :userid)
                                    (from :contacts))))
                   sql/format)))))

(describe "insert"
  (it "vector of maps"
    (should= ["INSERT INTO users (name, age) VALUES (?, 35), (?, 37)" "Billy" "Joey"]
             (-> (insert-into :users)
                 (values [{:name "Billy" :age 35}
                          {:name "Joey" :age 37}])
                 sql/format)))
  (it "vector of vectors"
    (should= ["INSERT INTO users VALUES (?, 35), (?, 37)" "Billy" "Joey"]
             (-> (insert-into :users)
                 (values [["Billy" 35]["Joey" 37]])
                 sql/format)))
  (it "modifiers"
    (should= ["INSERT IGNORE INTO users VALUES (?, 35), (?, 37)" "Billy" "Joey"]
             (-> (insert-into :users)
                 (modifiers :ignore)
                 (values [["Billy" 35]["Joey" 37]])
                 sql/format)))
  (it "insert select"
    (should= ["INSERT INTO foo (a, b, c) SELECT x.a, y.b, z.c FROM x INNER JOIN y ON x.id = y.id INNER JOIN z ON y.id = z.id"]
             (-> (insert-into :foo)
                 (columns :a :b :c)
                 (select :x.a :y.b :z.c)
                 (from :x)
                 (join :y '(= x.id y.id)
                       :z '(= y.id z.id))
                 sql/format)))
  (it "on duplicate key"
    (should= ["INSERT INTO foo (a, b, c) VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE c = 9"]
             (-> (insert-into :foo)
                 (columns :a :b :c)
                 (values [[1 2 3]])
                 (on-dup-key {:c 9})
                 sql/format)))
  (it "on duplicate key values"
    (should= ["INSERT INTO foo (a, b, c) VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE c = VALUES(a)"]
             (-> (insert-into :foo)
                 (columns :a :b :c)
                 (values [[1 2 3]])
                 (on-dup-key {:c (values' :a)})
                 sql/format))))

(describe "update"
  (it "setv"
    (should= ["UPDATE users SET name = ?, age = 35 WHERE id = 234" "Billy"]
             (-> (update :users)
                 (setv {:name "Billy", :age 35})
                 (where '(= id 234))
                 sql/format)))
  (it "modifiers"
    (should= ["UPDATE IGNORE items, month SET items.price = month.price WHERE items.id = months.id"]
             (-> (update :items :month)
                 (modifiers :ignore)
                 (setv '{items.price month.price})
                 (where '(= items.id months.id))
                 sql/format))))

(describe "delete"
  (it "single table"
    (should= ["DELETE FROM foo WHERE email = ?" "billy@bob.com"]
             (-> (delete-from :foo)
                 (where '(= email "billy@bob.com"))
                 sql/format)))
  (it "modifiers"
    (should= ["DELETE IGNORE FROM foo WHERE email = ?" "billy@bob.com"]
             (-> (delete-from :foo)
                 (modifiers :ignore)
                 (where '(= email "billy@bob.com"))
                 sql/format)))
  (it "multiple tables"
    (should= ["DELETE FROM t1 USING t1, t2 WHERE (t1.x = t2.x AND t2.y = 3)"]
             (-> (delete-from :t1)
                 (using :t1 :t2)
                 (where '(= t1.x t2.x)
                        '(= t2.y 3))
                 sql/format))))

(describe "replace"
  (it "vector of maps"
    (should= ["REPLACE INTO users (name, age) VALUES (?, 35), (?, 37)" "Billy" "Joey"]
             (-> (replace-into :users)
                 (values [{:name "Billy" :age 35}
                          {:name "Joey" :age 37}])
                 sql/format)))
  (it "vector of vectors"
    (should= ["REPLACE INTO users VALUES (?, 35), (?, 37)" "Billy" "Joey"]
             (-> (replace-into :users)
                 (values [["Billy" 35]["Joey" 37]])
                 sql/format)))
  (it "modifiers"
    (should= ["REPLACE IGNORE INTO users VALUES (?, 35), (?, 37)" "Billy" "Joey"]
             (-> (replace-into :users)
                 (modifiers :ignore)
                 (values [["Billy" 35]["Joey" 37]])
                 sql/format))))

(describe "quoting"
  (it "mysql"
    (should= ["SELECT `users`.`name`, `contacts`.*, date_format(`dob`, ?) FROM `users` INNER JOIN `contacts` ON `users`.`id` = `contacts`.`userid` WHERE (`users`.`status` IN (?, ?)) GROUP BY `users`.`status` ORDER BY `contacts`.`last_name` ASC LIMIT 25" "%m/%d/%Y" "active" "pending"]
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
                 (sql/format :quoting :mysql)))))

(describe "functions"
  (context "="
    (it "non-null values"
      (should= ["SELECT 1 = 1"]
               (-> (select '(= 1 1))
                   sql/format)))
    (it "first value is null"
      (should= ["SELECT x IS NULL"]
               (-> (select '(= nil x))
                   sql/format)))
    (it "second value is null"
      (should= ["SELECT x IS NULL"]
               (-> (select '(= x nil))
                   sql/format))))
  (context "<>"
    (context "aliases"
      (it "!="
        (should= ["SELECT 1 <> 2"]
                 (-> (select '(!= 1 2))
                     sql/format)))
      (it "not="
        (should= ["SELECT 1 <> 2"]
                 (-> (select '(not= 1 2))
                     sql/format))))
    (it "non-null values"
      (should= ["SELECT 1 <> 2"]
               (-> (select '(<> 1 2))
                   sql/format)))
    (it "first value is null"
      (should= ["SELECT x IS NOT NULL"]
               (-> (select '(<> nil x))
                   sql/format)))
    (it "second value is null"
      (should= ["SELECT x IS NOT NULL"]
               (-> (select '(<> x nil))
                   sql/format))))
  (it "not"
    (should= ["SELECT not(TRUE)"]
             (-> (select '(not true))
                 sql/format)))
  (it "is"
    (should= ["SELECT NULL IS NULL"]
             (-> (select '(is nil nil))
                 sql/format)))
  (it "is not"
    (should= ["SELECT FALSE IS NOT NULL"]
             (-> (select '(is-not false nil))
                 sql/format)))
  (it "<"
    (should= ["SELECT 1 < 2"]
             (-> (select '(< 1 2))
                 sql/format)))
  (it ">"
    (should= ["SELECT 2 > 1"]
             (-> (select '(> 2 1))
                 sql/format)))
  (it "<="
    (should= ["SELECT 1 <= 2"]
             (-> (select '(<= 1 2))
                 sql/format)))
  (it ">="
    (should= ["SELECT 2 >= 1"]
             (-> (select '(>= 2 1))
                 sql/format)))
  (it "between"
    (should= ["SELECT 2 BETWEEN 1 AND 3"]
             (-> (select '(between 2 1 3))
                 sql/format)))
  (it "not between"
    (should= ["SELECT 1 NOT BETWEEN 2 AND 3"]
             (-> (select '(not-between 1 2 3))
                 sql/format)))
  (it "like"
    (should= ["SELECT (firstName LIKE ?) FROM users" "bill"]
             (-> (select '(like firstName "bill"))
                 (from :users)
                 sql/format)))
  (it "not like"
    (should= ["SELECT (firstName NOT LIKE ?) FROM users" "bill"]
             (-> (select '(not-like firstName "bill"))
                 (from :users)
                 sql/format)))
  (context "in"
    (it "select clause"
      (should= ["SELECT (1 IN (1, 2))"]
               (-> (select '(in 1 [1 2]))
                   sql/format)))
    (it "where clause"
      (should= ["SELECT * FROM foo WHERE (1 IN (1, 2))"]
               (-> (select :*)
                   (from :foo)
                   (where '(in 1 [1 2]))
                   sql/format))))
  (context "not in"
    (it "select clause"
      (should= ["SELECT (1 NOT IN (1, 2))"]
               (-> (select '(not-in 1 [1 2]))
                   sql/format)))
    (it "where clause"
      (should= ["SELECT * FROM foo WHERE (1 NOT IN (1, 2))"]
               (-> (select :*)
                   (from :foo)
                   (where '(not-in 1 [1 2]))
                   sql/format))))
  (it "regexp"
    (should= ["SELECT (? REGEXP ?)" "Billy" "[A-Z]+"]
             (-> (select '(regexp "Billy" "[A-Z]+"))
                 sql/format)))
  (it "not regexp"
    (should= ["SELECT (? NOT REGEXP ?)" "Billy" "[A-Z]+"]
             (-> (select '(not-regexp "Billy" "[A-Z]+"))
                 sql/format)))
  (it "count-distinct"
    (should= ["SELECT COUNT(DISTINCT email) FROM users"]
             (-> (select '(count-distinct email))
                 (from :users)
                 sql/format))))

(describe "union"
  (it "multiple selects"
    (should= ["(SELECT name, email FROM users) UNION (SELECT name, email FROM deleted_users)"]
             (sql/format
               (union (-> (select :name :email)
                          (from :users))
                      (-> (select :name :email)
                          (from :deleted-users)))))))

(describe "complex"
  (it "nested in"
    (should= ["SELECT * FROM users WHERE (id IN (SELECT (id IN (1, 2)) FROM contacts))"]
             (-> (select :*)
                 (from :users)
                 (where `(in id
                             ~(-> (select '(in id [1 2]))
                                  (from :contacts))))
                 sql/format))))













