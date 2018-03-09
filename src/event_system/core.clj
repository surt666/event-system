(ns event-system.core
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
  (:require [amazonica.aws.s3 :refer [put-object]]
            [amazonica.aws.sns :refer [publish]]
            [amazonica.aws.dynamodbv2 :refer :all]
            [cheshire.core :refer :all]))

(def opgaver ["opgave-oprettet" "sagsbehandler-tilfoejet"])

(def vurderinger ["skoen-oprettet" "tillaeg-oprettet"])

(def sager ["sag-oprettet" "jp-oprettet" "jn-oprettet"])

(defn in? [coll item]
  (some #{item} coll))

(defn payload [e]
  (cond
    (= "sag-oprettet" (e :type)) {}
    (= "opgave-oprettet" (e :type)) {}
    (= "tillaeg-oprettet") (e :type)) {:entitet (e :entitet)
                                       :entitet-id (e :entitet-id)
                                       :del (e :del)
                                       :vaerdi (e :vaerdi)
                                       :kald-gvk (e :kald-gvk)
                                       :procent (e :procent)
                                       :begrundelse (e :begrundelse)
                                       :oprettet-af (e :oprettet-af)
                                       :termin (e :termin)
                                       :slut-termin  (e :slut-termin)
                                       :opgave-id (e :opgave-id)})

(defn write-event [e]
  (prn "E" e)
  (let [type (cond
               (in? sager (e :type)) "sags-events"
               (in? opgaver (e :type)) "opgave-events"
               (in? vurderinger (e :type)) "vur-events")]
    ;; (put-object :bucket-name "test-events-vur2"
    ;;             :key (str type "/" (e :vur-ejd-id) "/" (e :id))
    ;;             :input-stream (ByteArrayInputStream. (.getBytes (encode e))))
    (put-item :table-name type
              :return-consumed-capacity "TOTAL"
              :return-item-collection-metrics "SIZE"
              :item (if (= "sags-events" type)
                      {:sags-id (e :sags-id)
                       :tx (get-tx "opgaver")
                       :type (e :type)
                       :payload (payload e)}
                      {:vur-ejd-id (e :vur-ejd-id)
                       :tx (get-tx "opgaver")
                       :type (e :type)
                       :payload (payload e)}))
    ;; (publish :topic-arn "arn:aws:sns:eu-west-1:593176282530:EventReceived"
    ;;          :message (str (e :vur-ejd-id)))
    ))

(def -handleRequest (mk-req-handler write-event))
