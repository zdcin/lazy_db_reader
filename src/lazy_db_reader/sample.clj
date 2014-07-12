(ns lazy-db-reader.sample
  (:use lazy-db-reader.core)
  (:require [clojure.java.jdbc :as j])
  (:import [org.apache.hadoop.hbase HBaseConfiguration]
           [org.apache.hadoop.hbase.util Bytes]
           [org.apache.hadoop.hbase.client HTable Put])
  (:gen-class))


;(require '[clojure.java.jdbc :as j])

(defn my-load-fn [current_value]
  (let [mysql-db {:subprotocol "mysql"
                  :subname     ""
                  :user        ""
                  :password    ""}]
    (j/query mysql-db
             ["select pkg_name, mf_md5, cert_md5 from installed_apks where mf_md5>? order by mf_md5 asc limit 2000"
              (:mf_md5 current_value)])))



(defn to-put [item]
  (let [;_ (println item)
         row_key (Bytes/toBytes (str (:pkg_name item) "_" (:mf_md5 item)))
         family-name (Bytes/toBytes "apk")
         qulifier-name (Bytes/toBytes "cert_md5")
         value (Bytes/toBytes (:cert_md5 item))
         put (new Put row_key)]
    (. put add family-name qulifier-name value)))


(defn -main []
  (let [
         _ (println "in main...")
         my-table-seq (table-query-seq (->QueryState (ref {:mf_md5 ""}) (ref []) my-load-fn))
         ;__ (println (first my-table-seq))
         table (new HTable (HBaseConfiguration/create) "install_summary_mf")] ;install_summary_mf
    (dorun
      (map (fn [item-list]
             (let [puts
                   (map to-put item-list)]
               (. table put puts)))
           (partition 1000 1000 nil my-table-seq))          ;end map
                                                   ;			(. table close)
      )))