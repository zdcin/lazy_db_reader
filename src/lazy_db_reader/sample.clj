(ns lazy-db-reader.sample
  (:use lazy-db-reader.core)
  (:require [clojure.java.jdbc :as j])
  (:import [org.apache.hadoop.hbase HBaseConfiguration]
           [org.apache.hadoop.hbase.util Bytes]
           [org.apache.hadoop.hbase.client HTable Put])
  (:gen-class))

(def conf {
  :db-conf {:subprotocol "mysql"
           :subname     ""
           :user        ""
           :password    ""}
  :fetch-size 2000
  :query-init-value {:mf_md5 ""}
  :write-batch-size 1000
  :htable-name "install_summary_mf"
})

;(require '[clojure.java.jdbc :as j])

(defn my-load-fn [current_value]
  (j/query (:db-conf conf)
           ["select pkg_name, mf_md5, cert_md5 from installed_apks where mf_md5>? order by mf_md5 asc limit ?"
            (:mf_md5 current_value) (:fetch-size conf)]))

(defn to-put [item]
  (let [;_ (println item)
         row_key (Bytes/toBytes (str (:pkg_name item) "_" (:mf_md5 item)))
         family-name (Bytes/toBytes "apk")
         qulifier-name (Bytes/toBytes "cert_md5")
         value (Bytes/toBytes (:cert_md5 item))
         put (new Put row_key)]
    (. put add family-name qulifier-name value)))


(defn -main []
  (let [ batch-size (:write-batch-size conf)
         _ (println "in main...")
         my-table-seq (table-query-seq (->QueryState (ref {:mf_md5 ""}) (ref []) my-load-fn))
         ;__ (println (first my-table-seq))
         table (new HTable (HBaseConfiguration/create) (:htable-name conf))] ;install_summary_mf
    (dorun
      (map (fn [item-list]
             (let [puts (map to-put item-list)]
               (. table put puts)))
           (partition batch-size batch-size nil my-table-seq))          ;end map
                                                   ;			(. table close)
      )))