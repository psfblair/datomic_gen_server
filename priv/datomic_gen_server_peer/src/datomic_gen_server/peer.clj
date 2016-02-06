(ns datomic_gen_server.peer
  (:gen-class)
  (:require [clojure.core.async :as async :refer [<! >! <!! go close!]]
            [clojure-erlastic.core :refer [port-connection]]
            [clojure.core.match :refer [match]]
            [datomic.api :as datomic]
  ))

(defn- read-edn [edn-str]
  (clojure.edn/read-string {:readers *data-readers*} edn-str))

(defn- connect [db-url create?]
  (try 
    (datomic/connect db-url)
    (catch clojure.lang.ExceptionInfo e 
      (if (and create? (= :db.error/db-not-found (:db/error (ex-data e))))
        (do (datomic/create-database db-url)
          (datomic/connect db-url))
        (throw e)))))
  
(defn- q [database edn-str]
  (-> (datomic/q edn-str database) prn-str))

(defn- transact [connection edn-str]
  (let [completed-future (datomic/transact connection (read-edn edn-str))]
    @completed-future))

; TODO Improve exception handling here.
; Right now if the edn is ill-formed, the edn reader throws and everything blows up.
; Distinguish being unable to handle a message from being unable to contact the server.
; Handle unexpected messages too
; Returns the state of the database, or nil if shut down.
(defn- process-message [message database connection]
  (try
    (match message
      [:q edn] {:db database :result {:ok (q database edn)}}
      [:transact edn] (let [{:keys [db-after tx-data]} (transact connection edn)]
                        {:db db-after :result {:ok (prn-str tx-data)}})
      [:ping] {:db database :result {:ok (prn-str #{})}}
      [:exit] (do (datomic/shutdown true) nil)
      nil (do (datomic/shutdown true) nil)) ; Handle close of STDIN - parent is gone
    (catch Exception e {:db database :result {:error e}})))

(defn- exit-loop [in out] 
  (do
    (close! out) 
    (close! in) 
    :default))
  
(defn start-server 
  ([db-url in out] (start-server db-url in out false))
  ([db-url in out create?]
    (let [connection (connect db-url create?)]
      (<!! (go 
        (loop [database (datomic/db connection)]
          (let [message (<! in)
                result (process-message message database connection)]
            (if (some? result)
              (do
                (>! out (result :result))
                (recur (result :db)))
              (exit-loop in out))))))))) ; exit if we get a nil back from process-message.

(defn -main [& args]
  (cond
    (empty? args) (System/exit 1)
    (> (count args) 2) (System/exit 1)
    :else 
      (let [ port-config {:convention :elixir :str-detect :all}
             create-arg (second args)
             create? (and (some? create-arg) (.equalsIgnoreCase create-arg "true"))
             [in out] (clojure-erlastic.core/port-connection port-config)]
        (start-server (first args) in out create?))))
        
