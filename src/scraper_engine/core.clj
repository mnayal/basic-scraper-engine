(ns scraper-engine.core
  (:require [clj-http.client :as http]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io])
  (:import (java.text SimpleDateFormat)
           (java.util Date)))

(def shopee-api-url "https://shopee.sg/api/v4/recommend/recommend")
(def default-number-of-pages 3)
(def default-items-in-one-page 60)

(defn category-name-and-id [input]
  (let [category-string (last (clojure.string/split input #"/"))
        category-id (last (clojure.string/split category-string #"\."))]
    [category-string category-id]))

(defn request-params [category-id offset]
  {:accept :json
   :as :json
   :query-params {"bundle" "category_landing_page"
                  "catid" category-id
                  "limit" default-items-in-one-page
                  "offset" offset}})

(defn successful? [response]
  (= (:status response) 200))

(defn scrap-items [response-body]
  (let [section (first (get-in response-body [:data :sections]))
        items (get-in section [:data :item])]
    items))

(defn get-response [url request-params]
  (let [http-response (http/get url request-params)
        body (when (successful? http-response)
               (:body http-response))]
    (scrap-items body)))

(defn format-price [item]
  (update item :price (fn [price]
                        (->>
                          (/ price 100000)
                          (float)
                          (format "%.2f")))))

(defn parse-items [items page-number]
  (map-indexed (fn [index item]
                 (->
                   (select-keys item [:itemid :name :price])
                   (merge {:order-in-page index
                           :page-number   page-number})
                   (format-price)))
               items))

(defn write-to-csv [file rows append]
  (with-open [writer (io/writer file :append append)]
    (csv/write-csv writer rows)))

(defn create-csv [file data-with-page-number]
  (let [first-item (-> data-with-page-number first second)
        columns (-> first-item first keys)
        headers (mapv name columns)]
    (doseq [[page data] data-with-page-number]
      (let [rows (mapv #(mapv % columns) data)
            final-rows (if (= page 1)
                        (cons headers rows)
                        rows)
            append (not= page 1)]
        (write-to-csv file final-rows append)))))

(defn fetch-data [category-id number-of-pages]
  (pmap (fn [i]
         (let [page (+ i 1)
               offset (* i 60)
               request-params (request-params category-id offset)
               response (get-response shopee-api-url request-params)
               items (parse-items response page)]
           [page items]))
       (range number-of-pages)))

(defn csv-name-with-current-date-time [category-name]
  (let [current-date-time (.format (SimpleDateFormat. "dd-MMM-yyyy-HH:mm:ss") (new Date))]
    (format "generated/%s-At-%s.csv" category-name current-date-time)))

(defn scrap-data-for-category [category-url]
  (let [[category-name category-id] (category-name-and-id category-url)
        data-from-first-three-pages (fetch-data category-id default-number-of-pages)
        csv-file-name (csv-name-with-current-date-time category-name)]
    (create-csv csv-file-name data-from-first-three-pages)))

(defn -main []
  (with-open [reader (io/reader "resources/categoryUrls.txt")]
    (doseq [url (line-seq reader)]
      (scrap-data-for-category url))))
