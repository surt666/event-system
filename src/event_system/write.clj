(ns event-system.write
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
  (:require [amazonica.aws.s3 :refer [put-object]]
            [amazonica.aws.sns :refer [publish]]
            [amazonica.aws.dynamodbv2 :refer :all]
            [clojure.java.io :as io]
            [cheshire.core :refer :all]
            [clj-time.core :as t]
            [clj-time.format :as f]))

(def basic-formatter (f/formatters :basic-date-time))

(def opgaver ["opgave-oprettet" "sagsbehandler-tilfoejet"])

(def vurderinger ["skoen-oprettet" "tillaeg-oprettet" "nedslag-oprettet" "kvm-pris-oprettet"])

(def sager ["sag-oprettet""sag-opdateret" "jp-oprettet" "jp-opdateret" "jn-oprettet" "jn-opdateret" "dokument-oprettet" "dokument-opdateret" "part-oprettet" "part-opdateret"])

(defn command2event [c]
  (cond
    (= c "opret-skoen") "skoen-oprettet"
    (= c "opret-kvm-pris") "kvm-pris-oprettet"
    (= c "opret-tillaeg") "tillaeg-oprettet"
    (= c "opret-nedslag") "nedslag-oprettet"
    (= c "opret-opgave") "opgave-oprettet"
    (= c "tilfoej-sagsbehandler") "sagsbehandler-tilfoejet"))

(defn find-sag [data])

(defn find-ejendom [data])

(defn find-vurdering [data])

(defn query [action data]
  (cond
    (= action "find-sag") (find-sag data)
    (= action "find-ejendom") (find-ejendom data)
    (= action "find-vurdering") (find-vurdering data)))

(defn in? [coll item]
  (some #{item} coll))

(defn mk-req-handler
  "Makes a request handler"
  [f & [wrt]]
  (fn [this is os context]
    (let [w (io/writer os)
             res (-> (parse-stream (io/reader is) keyword)
                  f)]
      (prn "R" res)
      ((or wrt
           (fn [res w] (.write w (prn-str res))))
        res w)
      (.flush w))))

(defn get-tx [type]
  (get-in (update-item
           :table-name "tx-counter"
           :key {:type type}
           :update-expression "SET seq = seq + :incr"
           :expression-attribute-values {":incr" 1}
           :return-values "UPDATED_NEW") [:attributes :seq]))

(defn create-event [type data]
  (let [table (cond
               (in? sager type) "sags-events"
               (in? opgaver type) "opgave-events"
               (in? vurderinger type) "vur-events")
        item (cond
               (= "sags-events" table) {:type type
                                        :sags-id (data :sags-id)
                                        :tx (get-tx "sager")
                                        :oprettet (f/unparse basic-formatter (t/now))
                                        :payload data}
               (= "opgave-events" table) {:type type
                                          :vur-ejd-id (data :vur-ejd-id)
                                          :tx (get-tx "opgaver")
                                          :oprettet (f/unparse basic-formatter (t/now))
                                          :payload (dissoc data :vur-ejd-id)}
               (= "vur-events" table) {:type type
                                       :vur-ejd-id (data :vur-ejd-id)
                                       :tx (get-tx "vurderinger")
                                       :oprettet (f/unparse basic-formatter (t/now))
                                       :payload (dissoc data :vur-ejd-id)})]
       (put-item :table-name table
              :return-consumed-capacity "TOTAL"
              :return-item-collection-metrics "SIZE"
              :item item)))


(defn handle-events [body]
  (cond
    (= (get-in body [:context :resource-path]) "/sag") (create-event "sag-oprettet" (body :body-json))
    (= (get-in body [:context :resource-path]) "/sag/{sagsid}") (create-event "sag-opdateret" (assoc (body :body-json) :sags-id (get-in body [:params :path :sagsid])))
    (= (get-in body [:context :resource-path]) "/jp/{sagsid}") (create-event "jp-oprettet" (assoc (body :body-json) :sags-id (get-in body [:params :path :sagsid])))
    (= (get-in body [:context :resource-path]) "/jp/{sagsid}/{jpid}") (create-event "jp-opdateret" (assoc (body :body-json) :sags-id (get-in body [:params :path :sagsid]) :jp-id (get-in body [:params :path :jpid])))
    (= (get-in body [:context :resource-path]) "/jn/{sagsid}/{jpid}") (create-event "jn-oprettet" (assoc (body :body-json) :sags-id (get-in body [:params :path :sagsid]) :jp-id (get-in body [:params :path :jpid])))
    (= (get-in body [:context :resource-path]) "/jn/{sagsid}/{jpid}/{jnid}") (create-event "jn-opdateret" (assoc (body :body-json) :sags-id (get-in body [:params :path :sagsid]) :jp-id (get-in body [:params :path :jpid]) :jn-id :jp-id (get-in body [:params :path :jnid])))
    (= (get-in body [:context :resource-path]) "/dokument/{sagsid}/{jpid}") (create-event "dokument-oprettet" (assoc (body :body-json) :sags-id (get-in body [:params :path :sagsid]) :jp-id (get-in body [:params :path :jpid])))
    (= (get-in body [:context :resource-path]) "/dokument/{sagsid}/{jpid}/{dokid}") (create-event "dokument-opdateret" (assoc (body :body-json) :sags-id (get-in body [:params :path :sagsid]) :jp-id (get-in body [:params :path :jpid]) :jn-id :dok-id (get-in body [:params :path :dokid])))
    (= (get-in body [:context :resource-path]) "/part/{sagsid}") (create-event "part-oprettet" (assoc (body :body-json) :sags-id (get-in body [:params :path :sagsid])))
    (= (get-in body [:context :resource-path]) "/part/{sagsid}/{partid}") (create-event "part-opdateret" (assoc (body :body-json) :sags-id (get-in body [:params :path :sagsid]) :part-id (get-in body [:params :path :partid])))
    (= (get-in body [:context :resource-path]) "/command") (create-event (command2event (get-in body [:body-json :action])) (get-in body [:body-json :data]))
    (= (get-in body [:context :resource-path]) "/query") (query (get-in body [:body-json :action]) (get-in body [:body-json :data]))
    :default {:rp (get-in body [:context :resource-path])}))

(def -handleRequest (mk-req-handler handle-events))
