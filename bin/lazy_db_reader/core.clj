
(ns lazy-db-reader.core
  (:import [org.apache.hadoop.hbase HBaseConfiguration]
           [org.apache.hadoop.hbase.util Bytes]
           [org.apache.hadoop.hbase.client HTable Put])
  (:gen-class))

;定义读取db的协议
(defprotocol TableReader
  (has-more? [this] "有没有更多？")
  (load-more [this] "如果当前缓存用完，就再加载一批, 总是返回nil")
  (pop-one [this] "从缓存中弹出一条，没有的话，自动加载，没有更多返回nil"))

(defrecord QueryState [current_value cache load-fn])

;简化版
(extend QueryState
  TableReader
  {:has-more?  (fn [this]
                 (let [cache-ref (:cache this)
                       _cache (if (empty? @cache-ref)
                                (do (load-more this) @cache-ref)
                                @cache-ref)]
                   (not (empty? _cache))))
  :load-more  (fn [this]
                (let [value-ref (:current_value this)
                      cache-ref (:cache this)
                      load-more-fn
                      (fn [v-ref]
                        (let [c_value @v-ref
                              ;ret-list (list (+ c_value 1) (+ c_value 2))]
                              ret-list ((:load-fn this) c_value)
                              _no_use (println (last ret-list))]
                          (do
                            ;(println (str "inload, current_value=" c_value))
                            (dosync (ref-set v-ref (last ret-list)))
                            ret-list
                            )
                          ))]
                  (if (empty? @cache-ref)
                    ;加载并弹出一个
                    (dosync
                      (alter cache-ref concat
                             (load-more-fn value-ref)))))
                )

  :pop-one (fn [this]
                (let [cache-ref (:cache this)
                      my-pop-fn
                      (fn [c-ref]
                        (let [retv (first @c-ref)]
                          (do
                            (dosync (alter c-ref rest))
                            retv
                            )))]
                  (if (has-more? this)
                    ;加载并弹出一个
                    (my-pop-fn cache-ref)
                    )))})

(defn query-seq
  [query-state]
  (when-let [batch (pop-one query-state)]
    (cons batch (lazy-seq (query-seq query-state)))))

(require '[clojure.java.jdbc :as j])
  
(defn my-load-fn [current_value]
  (let [mysql-db {:subprotocol "mysql"
               :subname ""
               :user ""
               :password ""}]
  (j/query mysql-db
           ["select pkg_name, mf_md5, cert_md5 from installed_apks where mf_md5>? order by mf_md5 asc limit 2000" 
            (:mf_md5 current_value)])))



(defn to-put [item] 
  (let [;_ (println item)
         row_key (Bytes/toBytes (str (:pkg_name item) "_"  (:mf_md5 item)))
         family-name (Bytes/toBytes "apk")
         qulifier-name (Bytes/toBytes "cert_md5")
         value (Bytes/toBytes (:cert_md5 item))
         put (new Put row_key)]
       (. put add family-name qulifier-name value)))


(defn -main []
      (let [
            _ (println "in main...")
            my-table-seq (query-seq (->QueryState (ref {:mf_md5 ""}) (ref []) my-load-fn))
            ;__ (println (first my-table-seq))
            table (new HTable (HBaseConfiguration/create) "install_summary_mf")];install_summary_mf
        (dorun
          (map (fn [item-list]
						       (let [puts
						             (map to-put item-list)]
						         (. table put puts)))
						     (partition 1000 1000 nil my-table-seq));end map
			;			(. table close)
      )))

