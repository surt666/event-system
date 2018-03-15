(ns event-system.write
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
  (:require [amazonica.aws.s3 :refer [put-object]]
            [amazonica.aws.sns :refer [publish]]
            [amazonica.aws.dynamodbv2 :refer :all]
            [clojure.java.io :as io]
            [cheshire.core :refer :all]))

(def opgaver ["opgave-oprettet" "sagsbehandler-tilfoejet"])

(def vurderinger ["skoen-oprettet" "tillaeg-oprettet"])

(def sager ["sag-oprettet" "jp-oprettet" "jn-oprettet"])

(defn command2event [c]
  (cond
    (= c "opret-skoen") "skoen-oprettet"))

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

(defn get-tx [type]
  (get-in (update-item
           :table-name "tx-counter"
           :key {:type type}
           :update-expression "SET seq = seq + :incr"
           :expression-attribute-values {":incr" 1}
           :return-values "UPDATED_NEW") [:attributes :seq]))

(defn create-event [type data]
  (prn "TYPE" type)
  (let [table (cond
               (in? sager type) "sags-events"
               (in? opgaver type) "opgave-events"
               (in? vurderinger type) "vur-events")
        item (cond
               (= "sags-events" table) {:type type
                                        :sags-id (data :sags-id)
                                        :tx (get-tx "sager")
                                        :payload data}
               (= "opgave-events" table) {:type type
                                          :vur-ejd-id (data :vur-ejd-id)
                                          :tx (get-tx "opgaver")
                                          :payload data}
               (= "vur-events" table) {:type type
                                       :vur-ejd-id (data :vur-ejd-id)
                                       :tx (get-tx "vurderinger")
                                       :payload data})]
    (prn "ITEM" table item)
    (put-item :table-name table
              :return-consumed-capacity "TOTAL"
              :return-item-collection-metrics "SIZE"
              :item item)))


(defn handle-events [body]
  (prn "BODY" body (get-in body [:body-json :action]) (get-in body [:body-json :data]))
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
    :default {:rp (get-in body [:context :resource-path])}))

(def -handleRequest (mk-req-handler handle-events))

;; (defn payload [e]
;;   (cond
;;     (= "sag-oprettet" (e :type)) (handle-sags-events e)
;;     (= "opgave-oprettet" (e :type)) {}
;;     (= "tillaeg-oprettet") (e :type)) {:entitet (e :entitet)
;;                                        :entitet-id (e :entitet-id)
;;                                        :del (e :del)
;;                                        :vaerdi (e :vaerdi)
;;                                        :kald-gvk (e :kald-gvk)
;;                                        :procent (e :procent)
;;                                        :begrundelse (e :begrundelse)
;;                                        :oprettet-af (e :oprettet-af)
;;                                        :termin (e :termin)
;;                                        :slut-termin  (e :slut-termin)
;;                                        :opgave-id (e :opgave-id)})
