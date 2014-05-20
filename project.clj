(defproject stch-library/sql "0.1.1"
  :description
  "A DSL in Clojure for SQL query, DML, and DDL."
  :url "https://github.com/stch-library/sql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [stch-library/schema "0.3.3"]]
  :profiles {:dev {:dependencies [[speclj "3.0.2"]]}}
  :plugins [[speclj "3.0.2"]
            [codox "0.6.7"]]
  :codox {:src-dir-uri "https://github.com/stch-library/sql/blob/master/"
          :src-linenum-anchor-prefix "L"}
  :test-paths ["spec"])
