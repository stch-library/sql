(ns stch.sql
  "SQL DSL for query and data manipulation language
  (DML). Supports a majority of MySQL's statements."
  (:refer-clojure :exclude [update])
  (:require
   [stch.sql.types :as types]))

(defmulti build-clause (fn [name & args]
                         name))

(defmethod build-clause :default [_ m & args]
  m)

(defmacro defhelper [helper arglist & more]
  (let [kw (keyword (name helper))]
    `(do
       (defmethod build-clause ~kw ~(into ['_] arglist) ~@more)
       (defn ~helper [& args#]
         (let [[m# args#] (if (map? (first args#))
                            [(first args#) (rest args#)]
                            [{} args#])]
           (build-clause ~kw m# args#))))))

(defn collify [x]
  (if (coll? x) x [x]))

(defhelper select [m fields]
  (update-in m [:select] concat (collify fields)))

(defhelper replace-select [m fields]
  (assoc m :select (collify fields)))

(defhelper un-select [m fields]
  (update-in m [:select] #(remove (set (collify fields)) %)))

(defhelper from [m tables]
  (update-in m [:from] concat (collify tables)))

(defhelper replace-from [m tables]
  (assoc m :from (collify tables)))

(defn- prep-where [args]
  (let [[m preds] (if (map? (first args))
                    [(first args) (rest args)]
                    [{} args])
        [logic-op preds] (if (keyword? (first preds))
                           [(first preds) (rest preds)]
                           [:and preds])
        pred (if (= 1 (count preds))
               (first preds)
               (into [logic-op] preds))]
    [m pred logic-op]))

(defmethod build-clause :where [_ m pred]
  (if (nil? pred)
    m
    (assoc m :where (if (not (nil? (:where m)))
                      [:and (:where m) pred]
                      pred))))

(defn where [& args]
  (let [[m pred logic-op] (prep-where args)]
    (if (nil? pred)
      m
      (assoc m :where (if (not (nil? (:where m)))
                        [logic-op (:where m) pred]
                        pred)))))

(defmethod build-clause :replace-where [_ m pred]
  (if (nil? pred)
    m
    (assoc m :where pred)))

(defn replace-where [& args]
  (let [[m pred] (prep-where args)]
    (if (nil? pred)
      m
      (assoc m :where pred))))

(defhelper join [m clauses]
  (update-in m [:join] concat clauses))

(defhelper replace-join [m clauses]
  (assoc m :join clauses))

(defhelper left-join [m clauses]
  (update-in m [:left-join] concat clauses))

(defhelper replace-left-join [m clauses]
  (assoc m :left-join clauses))

(defhelper right-join [m clauses]
  (update-in m [:right-join] concat clauses))

(defhelper replace-right-join [m clauses]
  (assoc m :right-join clauses))

(defmethod build-clause :group-by [_ m fields]
  (update-in m [:group-by] concat (collify fields)))

(defn group [& args]
  (let [[m fields] (if (map? (first args))
                     [(first args) (rest args)]
                     [{} args])]
    (build-clause :group-by m fields)))

(defmethod build-clause :replace-group-by [_ m fields]
  (assoc m :group-by (collify fields)))

(defn replace-group [& args]
  (let [[m fields] (if (map? (first args))
                     [(first args) (rest args)]
                     [{} args])]
    (build-clause :replace-group-by m fields)))

(defmethod build-clause :having [_ m pred]
  (if (nil? pred)
    m
    (assoc m :having (if (not (nil? (:having m)))
                       [:and (:having m) pred]
                       pred))))

(defn having [& args]
  (let [[m pred logic-op] (prep-where args)]
    (if (nil? pred)
      m
      (assoc m :having (if (not (nil? (:having m)))
                         [logic-op (:having m) pred]
                         pred)))))

(defmethod build-clause :replace-having [_ m pred]
  (if (nil? pred)
    m
    (assoc m :having pred)))

(defn replace-having [& args]
  (let [[m pred] (prep-where args)]
    (if (nil? pred)
      m
      (assoc m :having pred))))

(defhelper order-by [m fields]
  (update-in m [:order-by] concat (collify fields)))

(defhelper replace-order-by [m fields]
  (assoc m :order-by (collify fields)))

(defn asc [field]
  [field :asc])

(defn desc [field]
  [field :desc])

(defhelper limit [m l]
  (if (nil? l)
    m
    (assoc m :limit (if (coll? l) (first l) l))))

(defhelper offset [m o]
  (if (nil? o)
    m
    (assoc m :offset (if (coll? o) (first o) o))))

(defhelper modifiers [m ms]
  (if (nil? ms)
    m
    (update-in m [:modifiers] concat (collify ms))))

(defhelper replace-modifiers [m ms]
  (if (nil? ms)
    m
    (assoc m :modifiers (collify ms))))

(defn union
  [& select-stmts]
  {:union select-stmts})

(defmethod build-clause :insert-into [_ m table]
  (assoc m :insert-into table))

(defn insert-into
  ([table] (insert-into nil table))
  ([m table] (build-clause :insert-into m table)))

(defmethod build-clause :replace-into [_ m table]
  (assoc m :replace-into table))

(defn replace-into
  ([table] (replace-into nil table))
  ([m table] (build-clause :replace-into m table)))

(defhelper columns [m fields]
  (update-in m [:columns] concat (collify fields)))

(defhelper replace-columns [m fields]
  (assoc m :columns (collify fields)))

(defmethod build-clause :replace-values [_ m vs]
  (assoc m :values vs))

(defn replace-values
  ([vs] (replace-values nil vs))
  ([m vs] (build-clause :values m vs)))

(defmethod build-clause :values [_ m vs]
  (update-in m [:values] concat vs))

(defn values
  ([vs] (values nil vs))
  ([m vs] (build-clause :values m vs)))

(defmethod build-clause :query-values [_ m vs]
  (assoc m :query-values vs))

(defn query-values
  ([vs] (query-values nil vs))
  ([m vs] (build-clause :query-values m vs)))

(defhelper update [m tables]
  (assoc m :update tables))

(defmethod build-clause :set [_ m values]
  (assoc m :set values))

(defn setv
  ([vs] (setv nil vs))
  ([m vs] (build-clause :set m vs)))

(defmethod build-clause :on-dup-key [_ m values]
  (assoc m :on-dup-key values))

(defn on-dup-key
  ([vs] (on-dup-key nil vs))
  ([m vs] (build-clause :on-dup-key m vs)))

(defn values' [x]
  (types/raw (str "VALUES(" (name x) ")")))

(defhelper delete-from [m tables]
  (assoc m :delete-from tables))

(defhelper using [m tables]
  (update-in m [:using] concat (collify tables)))

