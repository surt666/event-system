(ns event-system.generate-events
  (:require [event-system.write :as ew]))

(defn create-event [id type opgave vur-ejd-id postnr sagsbehandler start slut]
  (cond
    (= type "opgave-oprettet") {:type type
                                :vur-ejd-id vur-ejd-id
                                :opgave opgave
                                :postnr postnr
                                :opgave-id id}
    (= type "opgave-lukket") {:type type
                              :vur-ejd-id vur-ejd-id
                              :opgave-id id}
    (= type "opgave-sagsbehandler-tilfoejet") {:type type
                                               :vur-ejd-id vur-ejd-id
                                               :sagsbehandler sagsbehandler
                                               :opgave-id id}
    (= type "opgave-startet") {:type type
                             :vur-ejd-id vur-ejd-id
                             :opgave-id id
                             :start start}
    (= type "opgave-stoppet") {:type type
                               :vur-ejd-id vur-ejd-id
                               :opgave-id id
                               :slut slut}))

(defn generate-opgaver []
  (let [typer ["opgave-oprettet" "opgave-sagsbehandler-tilfoejet" "opgave-lukket"]
        sagsbehandlere ["w19807" "w12727" "w82828" "w67238"]
        postnr [1200 1300 2200 2000 4600 8000 5000 2300 2100]
        opgaver ["kontrol-model" "kontrol-data" "manuel-vurdering" "juridisk-kategori"]
        vur-ejd-ids (vec (range 30))
        oprettet (mapv #(create-event % "opgave-oprettet" (get opgaver (rand-int (count opgaver))) (get vur-ejd-ids (rand-int (count vur-ejd-ids))) (get postnr (rand-int (count postnr))) (get sagsbehandlere (rand-int (count sagsbehandlere)))) (range  308 358))
        sagsbehandler (mapv #(create-event (:opgave-id %) "opgave-sagsbehandler-tilfoejet" nil (:vur-ejd-id %) nil (get sagsbehandlere (rand-int (count sagsbehandlere)))) (take 30 oprettet))
        startet (mapv #(create-event (:opgave-id %) "opgave-startet" nil (:vur-ejd-id %) nil nil 1523404800) sagsbehandler)
        stoppet (mapv #(create-event (:opgave-id %) "opgave-stoppet" nil (:vur-ejd-id %) nil nil (= 1523404800 (rand-int 1000))) sagsbehandler)
        lukket (mapv #(create-event (:opgave-id %) "opgave-lukket" nil (:vur-ejd-id %) nil nil) sagsbehandler)]
    (mapv #(ew/create-event (:type %) %) oprettet)
    (mapv #(ew/create-event (:type %) %) sagsbehandler)
    (mapv #(ew/create-event (:type %) %) startet)
    (mapv #(ew/create-event (:type %) %) stoppet)
    (mapv #(ew/create-event (:type %) %) lukket)))
