(defproject cantata "0.1.9"
  :description "SQL and database abstraction"
  :url "https://github.com/jkk/cantata"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/core.cache "0.6.3"]
                 [org.flatland/ordered "1.5.1"]
                 [honeysql "0.4.3"]
                 [org.clojure/java.jdbc "0.3.0-rc1"]
                 [c3p0/c3p0 "0.9.1.2"]
                 [joda-time "2.2"]]
  :profiles {:dev {:resource-paths ["dev-resources"]
                   :dependencies [[mysql/mysql-connector-java "5.1.26"]
                                  [org.postgresql/postgresql "9.2-1003-jdbc4"]
                                  [com.h2database/h2 "1.3.173"]
                                  [org.clojure/tools.logging "0.2.6"]
                                  [log4j "1.2.17" :exclusions [javax.mail/mail
                                                               javax.jms/jms
                                                               com.sun.jdmk/jmxtools
                                                               com.sun.jmx/jmxri]]
                                  [org.slf4j/slf4j-log4j12 "1.7.5"]]}})
