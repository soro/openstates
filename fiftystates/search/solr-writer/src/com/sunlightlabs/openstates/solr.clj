(ns com.sunlightlabs.openstates.solr
  (:import (org.apache.solr.request QueryResponseWriter))
  (:use clojure.contrib.json)
  (:gen-class
   :name com.sunlightlabs.openstates.solr.JSONWriter
   :implements [org.apache.solr.request.QueryResponseWriter]
   :init initialize-state
   :state state))

(defn -initialize-state []
  [[] (ref {})])

(defn -init [this args])

(defn -getContentType [this request response]
  "application/json")

(defn get-doc
  "Grabs a Solr document from searcher by id and extracts the requested fields
  into a hash."
  [id searcher fields]
  (let [doc (.doc searcher id)]
    (apply hash-map (flatten (map #(list %1 (.get doc %1)) fields)))))

(defn -write [this writer request response]
  (let [doc-ids (->> (.getValues response)
                     .iterator
                     iterator-seq
                     second
                     .getValue
                     .iterator
                     iterator-seq)  ; Java APIs, wheeee
        return-fields (.getReturnFields response)
        searcher (.getSearcher request)]
    (binding [*out* writer]
      (pprint-json (map #(get-doc %1 searcher return-fields) doc-ids)))))