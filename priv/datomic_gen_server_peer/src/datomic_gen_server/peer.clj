(ns datomic_gen_server.peer
  (:gen-class)
  (:require [clojure.core.async :as async :refer [<! >! <!! go close!]]
            [clojure-erlastic.core :refer [port-connection]]
            [clojure.core.match :refer [match]]
            [datomic.api :as datomic]
            [datomock.core :as datomock]
            [net.phobot.datomic.migrator :refer [run-migrations]]
            [net.phobot.datomic.seed :refer [transact-seed-data]]
  ))

; TODO Maybe want to configure this
; The amount of time to wait after a migration for a db sync to occur.
(def migration-timeout-ms 5000)

(defn- connect [db-url create?]
  (try 
    (datomic/connect db-url)
    (catch clojure.lang.ExceptionInfo e 
      (if (and create? (= :db.error/db-not-found (:db/error (ex-data e))))
        (do (datomic/create-database db-url)
          (datomic/connect db-url))
        (throw e)))))

(defn- read-edn [edn-str]
  (clojure.edn/read-string {:readers *data-readers*} edn-str))

;; This allows us to bind datomic_gen_server.peer/*db* in the edn that is passed in
(declare ^:dynamic *db*)
(defn- q [database edn-str binding-edn-list]
  (if (empty? binding-edn-list)
      (let [result (-> edn-str (datomic/q database) prn-str)]
        result)
    (binding [*db* database]
      (let [result (->> binding-edn-list (map read-edn) (map eval) (apply datomic/q edn-str) prn-str)]
        result))))

(defn- transact [connection edn-str]
  (let [completed-future (datomic/transact connection (read-edn edn-str))]
    @completed-future))
    
(defn- with [database edn-str db-edn]
  (binding [*db* database]
    (let [as-of-db (-> db-edn read-edn eval)
          result (datomic/with as-of-db (read-edn edn-str))]
      result)))

(defn- entity-attributes [attribute-names entity-map]
  (let [attrs (if (= :all attribute-names)
                (keys entity-map)
                attribute-names)
        selected (select-keys entity-map attrs)]
    (select-keys entity-map attrs)))
                
(defn- entity [database edn-str attr-names]
  (->> (read-edn edn-str) (datomic/entity database) (entity-attributes attr-names) prn-str))
    
(defn- serialize-datoms [datom]
  {:a (.a datom) :e (.e datom) :v (.v datom) :tx (.tx datom) :added (.added datom) })

(defn- serialize-transaction-response [transaction-response]
  (let [db-before (transaction-response :db-before)
        before-basis-t (datomic/basis-t db-before)
        db-after (transaction-response :db-after)
        after-basis-t (datomic/basis-t db-after)
        tx-data (transaction-response :tx-data)]
    (prn-str 
      { :db-before {:basis-t before-basis-t}
        :db-after {:basis-t after-basis-t}
        :tx-data (into [] (map serialize-datoms tx-data))
        :tempids (transaction-response :tempids)
      })))
      
(defn- migrate [connection migration-path]
  ;; TODO Figure out a better way to handle logging
  (let [logger-fn (fn [& args] nil)]
    (run-migrations connection migration-path logger-fn)
    ; run-migrations calls doseq, which returns nil, so migrate does not supply a db-after.
    {:db-after (deref (datomic/sync connection) migration-timeout-ms nil)}))

(defn- load-data [connection data-resource-path]
  ;; TODO Figure out a better way to handle logging
  (let [logger-fn (fn [& args] nil)
        completed-future (transact-seed-data connection data-resource-path logger-fn)]
    @completed-future))

(defn- mock-connection [starting-point-db active-connection db-map db-key]
  (if (Boolean/getBoolean "datomic.mocking")
    (let [mocked-conn (datomock/mock-conn starting-point-db)
          updated-db-map (assoc db-map db-key starting-point-db)]
      {:db starting-point-db :connection mocked-conn :db-map updated-db-map})
    {:db starting-point-db :connection active-connection :db-map db-map}
  ))
  
(defn- reset-to-mock [db-map db-key active-db active-connection]
  (if (Boolean/getBoolean "datomic.mocking")
    (let [starting-point-db (db-map db-key)
          mocked-conn (datomock/mock-conn starting-point-db)]
      {:db starting-point-db :connection mocked-conn :db-map db-map})
    {:db active-db :connection active-connection :db-map db-map}
  ))
  
(defn- new-state [result active-db-value active-connection db-map]
  { :result result 
    :active-db active-db-value 
    :active-connection active-connection
    :db-snapshots db-map
  })
  
; Returns the result along with the state of the database, or nil if shut down.
; Results are vectors starting with :ok or :error so that they go back to Elixir
; as the corresponding tuples.
(defn- process-message [message database connection db-map real-connection]
  (try
    (if (Boolean/getBoolean "debug.messages") (.println *err* (str "PEER RECEIVED: [" message "]")) :default)
    (match message
      ; IMPORTANT: RETURN MESSAGE ID IF IT IS AVAILABLE
      [:q message-id edn binding-edn]
          (let [response [:ok message-id (q database edn binding-edn)]]
            (new-state response database connection db-map))
      [:entity message-id edn attr-names]
          (let [response [:ok message-id (entity database edn attr-names)]]
            (new-state response database connection db-map))
      [:transact message-id edn] 
          (let [transaction-result (transact connection edn)
                db-after (transaction-result :db-after)
                response [:ok message-id (serialize-transaction-response transaction-result)]]
            (new-state response db-after connection db-map))
      [:migrate message-id migration-path] 
          (let [transaction-result (migrate connection migration-path)
                db-after (transaction-result :db-after)
                response [:ok message-id :migrated]]
            (new-state response db-after connection db-map))
      [:load message-id data-resource-path] 
          (let [transaction-result (load-data connection data-resource-path)
                db-after (transaction-result :db-after)
                response [:ok message-id (serialize-transaction-response transaction-result)]]
            (new-state response db-after connection db-map))
      [:mock message-id db-key] 
          (let [{new-db :db mock-connection :connection updated-db-map :db-map} 
                  (mock-connection database connection db-map db-key)
                response [:ok message-id db-key]]
            (new-state response new-db mock-connection updated-db-map))
      [:reset message-id db-key] 
          (let [{new-db :db mock-connection :connection updated-db-map :db-map}
                  (reset-to-mock db-map db-key database connection)
                response [:ok message-id db-key]]
            (new-state response new-db mock-connection updated-db-map))
      [:unmock message-id]             
          (let [real-db (datomic/db real-connection)
                response [:ok message-id :unmocked]]
            (new-state response real-db real-connection db-map))
      [:ping]
          (let [response [:ok :ping]]
            (new-state response database connection db-map))
      [:stop] (do (datomic/shutdown false) nil) ; For testing from Clojure; does not release Clojure resources
      [:exit] (do (datomic/shutdown true) nil) ; Shuts down Clojure resources as part of JVM shutdown
      nil (do (datomic/shutdown true) nil)) ; Handle close of STDIN - parent is gone
    (catch Exception e 
      (let [response [:error message e]]
        (if (Boolean/getBoolean "debug.messages") (.println *err* (str "PEER EXCEPTION: [" response "]")) :default)
        (new-state response database connection db-map)))))

(defn- exit-loop [in out] 
  (do
    (close! out) 
    (close! in) 
    :default))
  
(defn start-server 
  ([db-url in out] (start-server db-url in out false))
  ([db-url in out create?]
    (let [real-connection (connect db-url create?)]
      (<!! (go 
        (loop [database (datomic/db real-connection)
               active-connection real-connection ;allows mocking
               db-map {}]
          (let [message (<! in)
                result (process-message message database active-connection db-map real-connection)]
            (if (some? result)
              (do
                (if (Boolean/getBoolean "debug.messages") (.println *err* (str "PEER REPLY: [" result "]")) :default)
                (>! out (result :result))
                (recur (result :active-db)(result :active-connection)(result :db-snapshots)))
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
        
; TODO:
; 1. When we send the :mock message, we:
;   a. Check system property to see if mocking is enabled #(Boolean/getBoolean "datomic.mocking") 
;   b. If mocking is not enabled, return 
;             the current db, 
;             the real connection as the "active" one
;             the unchanged map of db values, and
;             the real connection 
;   c. Otherwise, we create a new mock connection with the current db value
;   d. Save the db value as a "starting-point" db in a map of dbs with the key passed by mock
;   e. Return
;             the "starting-point" db, 
;             the mocked connection as the "active" one
;             the map of db values, and 
;             the real connection 
;   
; 2. When we send a new message we continue to use the current db and connection,
;     but have to pass those same 4 things out of the loop.
; 
; 3. When we send a :reset message, we
;   a. Check system property to see if mocking is enabled #(Boolean/getBoolean "datomic.mocking") 
;   b. If mocking is not enabled, return the current db, the map, the real connection, and the real connection as the active one
;   c. Use the key in the :reset message to get the db we are resetting to
;   d. Create a new mocked connection from the db.
;   e. Return
;             the db that you reset to, 
;             the mocked connection as the "active" one
;             the map of db values, and 
;             the real connection 
;
; 4. When we send an :unmock message, we
;   a. We use the real connection and get the db from it.
;   b. Return
;             the real db, 
;             the real connection as the "active" one
;             the unchanged map of db values, and 
;             the real connection
