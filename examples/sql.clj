;; To be run in a REPL


; DSL
(require '[stch.sql.format :as sql])
(use 'stch.sql.dsl)

(-> (union (-> (select :name :email)
               (from :users))
           (-> (select :name :email)
               (from :deleted-users)))
    sql/format)

(-> (update :foo)
    (setv {:name "Billy", :age 35})
    (where '(= userID 234))
    sql/format)

(-> (update :items :month)
    (setv '{items.price month.price})
    (where '(= items.id months.id))
    sql/format)

(-> (update :items :month)
    (modifiers :ignore)
    (setv '{items.price month.price})
    (where '(= items.id months.id))
    sql/format)

(-> (delete-from :foo)
    (where '(= email "billy@bob.com"))
    sql/format)

(-> (delete-from :foo)
    (modifiers :ignore)
    (where '(= email "billy@bob.com"))
    sql/format)

(-> (delete-from :t1)
    (using :t1 :t2)
    (where '(= t1.x t2.x)
           '(= t2.y 3))
    sql/format)

(-> (insert-into :foo)
    (columns :name :age)
    (values [["Billy" 35] ["Joey" 37]])
    sql/format)

(-> (insert-into :foo)
    (values [{:name "Billy" :age 35}
             {:name "Joey" :age 37}])
    sql/format)

(-> (insert-into :foo)
    (modifiers :ignore)
    (values [{:name "Billy" :activated '(now)}])
    sql/format)

(-> (insert-into :foo)
    (columns :a :b :c)
    (select :x.a :y.b :z.c)
    (from :x)
    (join :y '(= x.id y.id)
          :z '(= y.id z.id))
    sql/format)

(-> (insert-into :foo)
    (columns :a :b :c)
    (values [[1 2 3]])
    (on-dup-key {:c 9})
    sql/format)

(-> (insert-into :foo)
    (columns :a :b :c)
    (values [[1 2 3]])
    (on-dup-key {:c (values' :a)})
    sql/format)

(-> (replace-into :foo)
    (columns :a :b :c)
    (values [[1 2 3]])
    sql/format)

(-> (select :*)
    (from :foo)
    (where '(= firstName "Billy"))
    (where '(= lastName "Bob"))
    sql/format)

(-> (select [:firstName :name])
    (from [:systemUsers :users])
    (where '(and (= firstName "Billy")
                 (= lastName "Bob")))
    (group '(count some_field))
    sql/format)

(let [match-name
      '((= firstName "Billy")
        (= lastName "Bob"))
      match-alias "billy-bob"]
  (-> (select [:firstName :name])
      (from [:systemUsers :users])
      (where `(or (and ~@match-name)
                  (= alias ~match-alias)))
      (group '(count some_field))
      sql/format))

(-> (select :*)
    (from :users)
    (where '(in id [1 2 3]))
    sql/format)

(let [ids [1 2 3]]
  (-> (select :*)
      (from :users)
      (where `(not-in id ~ids))
      sql/format))

(-> (select '(now))
    sql/format)

(-> (select '(count-distinct id))
    sql/format)

(-> (select '(<> 1 2)) sql/format)

(-> (select
     '(concat
       (if (= gender "M")
         "Mr." "Ms.")
       lastName))
    sql/format)

(-> (select '(<> 1 (<> 2 3)))
    sql/format)

(let [field :id]
  (-> (apply select [field :name :email])
      (from :foo)
      sql/format))

(let [n "Billy"]
  (-> (select :*)
      (from :foo)
      (join :bar '(= foo.x bar.x))
      (join [:baz :b] '(= bar.x b.x))
      (where `(= name ~n))
      sql/format))

(-> (select :*)
    (from :users)
    (join :contacts
          '(= users.id contacts.id))
    sql/format)

(-> (select :*)
    (from :foo)
    (join :bar '(= foo.x bar.x)
          [:baz :b] '(= bar.x b.x))
    sql/format)

(-> (select :*)
    (from :foo)
    (join :bar '(and (= foo.x bar.x)
                     (= foo.y bar.y)))
    sql/format)

(-> (select :foo.name)
    (from :foo)
    (where '(= activated (curdate))
           '(= userID 3))
    (sql/format))

(-> (select :foo.name)
    (from :foo)
    (where '(= deactivateDate nil))
    (sql/format))

(-> (select :foo.name)
    (from :foo)
    (where '(is deactivateDate nil))
    (sql/format))

(-> (select :foo.name)
    (from :foo)
    (where '(is-not deactivateDate nil))
    (sql/format))

(-> (select :foo.name, :?name)
    (from :foo)
    (where '(= name ?name)
           '(= userID ?userID))
    (sql/format :params {:name "Billy"
                         :userID 3}))

(-> (select :foo.name)
    (from :foo)
    (where '(= name ?name)
           '(= userID ?userID))
    (sql/format :params ["Billy" 3]))

(-> (select :foo.name)
    (from :foo)
    (where '(= name ?)
           '(= userID ?))
    (sql/format :params ["Billy" 3]))

(-> (select :*)
    (from :foo)
    (order-by [:name :desc] :activated)
    sql/format)

(-> (select :*)
    (from :foo)
    (order-by [:name 'desc])
    sql/format)

(-> (select :*)
    (from :foo)
    (order-by (desc :name))
    sql/format)

(-> (select :*)
    (from :foo)
    (order-by (asc :name))
    sql/format)

(-> (select :userID)
    (from :foo)
    (order-by '(sum totalSteps))
    sql/format)

(-> (select :*)
    (from :foo)
    (group :org :group)
    sql/format)

(-> (select :*)
    (from :foo)
    (from :bar)
    sql/format)

(-> (select :*)
    (select :name)
    (from :foo)
    sql/format)

(-> (select :*)
    (from :foo)
    (where '(= firstName "Billy"))
    (where '(= lastName "Bob"))
    sql/format)

(-> (select :*)
    (from :foo)
    (join :bar '(= foo.x bar.x))
    (join :baz '(= bar.x baz.x))
    sql/format)

(-> (select :*)
    (from :foo)
    (group :firstName)
    (group :lastName)
    sql/format)

(-> (select :*)
    (from :foo)
    (order-by :firstName)
    (order-by :lastName)
    sql/format)

(-> (select :*)
    (from :foo)
    (having '(> (count this) 2))
    (having '(> (count that) 2))
    sql/format)

(-> (select '(now)
            '(sum points)
            '(date_format dob "%m/%d/%Y")
            :email
            [:artists.name :aname])
    (from [:foo :f])
    (join :draq '(= f.b draq.x))
    (join [:artists :a]
          '(= foo.artist_id a.artist_id))
    (where '(= a ?baz)
           '(!= b "Elvis")
           '(or (= c "Beatles")
                (= c "Rolling Stones")))
    (group :foo.id '(date activated))
    (having '(< f.e 50))
    (order-by (asc :c.quux))
    (limit 50)
    (sql/format :params {:baz "BAZ"}
                :quoting :mysql))

; DDL
(use 'stch.sql.ddl)

(create (db :prod) (character-set :utf8))

(alt-db :prod (character-set :utf8))

(drop-db :prod)
(drop-db (db :prod))

(-> (columns)
    (integer :userID :unsigned :not-null))

(defcolumns user-id
  (integer :userID :unsigned :not-null)
  (index :userID))

(defcolumns org-id
  (integer :orgID :unsigned :not-null)
  (index :orgID))

(-> (table :contest)
    (append user-id)
    (append org-id))

(deftable contest
  (append user-id)
  (append org-id))

(create
  (-> (table :users)
      (set' :groups ["user" "admin"] (default "user"))
      (enum :status ["active" "inactive"])
      (decimal :ranking '(3 1) (default 0))
      (chr :countryCode [2] (default "US"))))

(create
  (-> (table :users)
      (integer :userID :unsigned :not-null :primary-key)
      (varchar :username [50])
      (constraint :uname (unique :username))
      (constraint :fk1 (foreign-key :userID
                                    '(users userID)
                                    :on-delete-cascade))))

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

(alt (-> (table :system-users)
         (rename :users)))

(def first-name (varchar :firstName [50]))

(create
  (-> (table :users)
      (append first-name)
      (index first-name)))

(defcolumns full-name
  (varchar :firstName [50])
  (varchar :lastName [50]))

(create
  (-> (table :users)
      (append full-name)
      (index full-name)))

(alt
  (-> (table :users)
      (add first-name)))

(alt
  (-> (table :users)
      (add full-name)))

