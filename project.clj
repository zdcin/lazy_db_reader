(defproject lazy_db_reader "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main lazy-db-reader.core
  :aot [lazy-db-reader.core]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 ;; MySQL
                 [mysql/mysql-connector-java "5.1.25"]
                 ;;简单的sql库, 现在叫clojure.java.jdbc
                 ;;[org.clojure/clojure-contrib "1.2.0"]
                 [org.clojure/java.jdbc "0.3.3"]
                 [org.apache.hbase/hbase-client "0.98.3-hadoop2"]
                 ])
