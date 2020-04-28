(defproject stch-library/sql "0.1.4"
  :description
  "A DSL in Clojure for SQL query, DML, and DDL."
  :url "https://github.com/stch-library/sql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[stch-library/schema "0.3.3"]]
  :profiles
  {:1.8 {:dependencies [[org.clojure/clojure "1.8.0"]]}
   :1.9 {:dependencies [[org.clojure/clojure "1.9.0"]]}
   :1.10 {:dependencies [[org.clojure/clojure "1.10.1"]]}
   :dev [:1.10
         {:dependencies [[speclj "3.0.2"]]}]}
  :plugins [[speclj "3.0.2"]
            [codox "0.6.7"]]
  :codox {:src-dir-uri "https://github.com/stch-library/sql/blob/master/"
          :src-linenum-anchor-prefix "L"}
  :test-paths ["spec"]
  :aliases {"test-all" ["with-profile" "1.8,dev:1.9,dev:1.10,dev" "spec"]})
