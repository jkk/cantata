(defproject cantata "0.1.0-SNAPSHOT"
  :description "Database abstraction library"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.flatland/ordered "1.5.1"]
                 [honeysql "0.4.2"]
                 [org.clojure/java.jdbc "0.3.0-alpha4"]
                 [c3p0/c3p0 "0.9.1.2"]
                 [joda-time "2.2"]]
  :profiles {:dev {:resource-paths ["dev-resources"]
                   :dependencies [[mysql/mysql-connector-java "5.1.26"]
                                  [org.postgresql/postgresql "9.2-1003-jdbc4"]
                                  [com.h2database/h2 "1.3.173"]]}})
