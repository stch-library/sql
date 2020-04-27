(ns stch.sql.util
  "Shared utility fns for query, DML, and DDL."
  (:require
   [clojure.string :as string]))

(defn pad [s]
  (str " " s " "))

(defn comma-join [s]
  (string/join ", " s))

(defn space-join [s]
  (string/join " " s))

(defn paren-wrap [x]
  (str "(" x ")"))

(defn undasherize [s]
  (string/replace s "-" "_"))
