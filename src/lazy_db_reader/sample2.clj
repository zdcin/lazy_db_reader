(ns lazy-db-reader.sample2
  (:use lazy-db-reader.core)
  (:gen-class))

  (def my-lazy-seq
    (table-query-seq
      (->QueryState
        (ref 33)
        (ref [])
        (fn [value]
          (list (+ value 1) (+ value 2))))))