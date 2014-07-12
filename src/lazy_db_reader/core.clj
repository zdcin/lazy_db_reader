(ns lazy-db-reader.core
  (:gen-class))

;定义读取db的协议
(defprotocol TableReader
  (has-more? [this] "有没有更多？")
  (load-more [this] "如果当前缓存用完，就再加载一批, 总是返回nil")
  (pop-one [this] "从缓存中弹出一条，没有的话，自动加载，没有更多返回nil"))


;TableReader的实现，这种类型记录的变化的状态，会有副作用
;
;value-ref 记录当前查询的状态，初始值手动设置，之后每次执行load-fn之后，都会把返回的结果列表最后一条放到value-ref中
;cache-ref 用于存储小批量加载数据的临时缓存
;load-fn  加载下一批数据的方法，输入是value-ref，输出放到cache-ref中
(deftype QueryState [value-ref cache-ref load-fn]
  TableReader
  (has-more? [this]
    (let [_cache (if (empty? @cache-ref)
                   (do (load-more this) @cache-ref)
                   @cache-ref)]
      (not (empty? _cache))))

  (load-more [this]
    (let [load-more-fn
          (fn [v-ref]
            (let [c_value @v-ref
                  ;ret-list (list (+ c_value 1) (+ c_value 2))]
                  ret-list (load-fn c_value)]
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
                 (load-more-fn value-ref))))))

  (pop-one [this]
    (let [my-pop-fn
          (fn [c-ref]
            (let [retv (first @c-ref)]
              (do
                (dosync (alter c-ref rest))
                retv
                )))]
      (if (has-more? this)
        ;加载并弹出一个
        (my-pop-fn cache-ref)
        ))))

(defn table-query-seq
  "返回一个由数据库记录组成的lazy seq，输入是一个实现了延迟加载的TableReader"
  {:static true}
  [^QueryState query-state]

  (when-let [batch (pop-one query-state)]
    (cons batch (lazy-seq (table-query-seq query-state)))))

