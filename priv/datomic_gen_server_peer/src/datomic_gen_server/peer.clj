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

(defn- load-data [db-url connection data-resource-path]
  ;; TODO Figure out a better way to handle logging
  (let [logger-fn (fn [& args] nil)
        completed-future (transact-seed-data connection data-resource-path logger-fn)]
    @completed-future))
  
; Returns the result along with the state of the database, or nil if shut down.
; Results are vectors starting with :ok or :error so that they go back to Elixir
; as the corresponding tuples.
(defn- process-message [message database connection db-url]
  (try
    (match message
      ; IMPORTANT: RETURN MESSAGE ID IF IT IS AVAILABLE
      [:q id edn binding-edn] {:db database :result [:ok id (q database edn binding-edn)]}
      [:entity id edn attr-names] {:db database :result [:ok id (entity database edn attr-names)]}
      [:transact id edn] 
          (let [result (transact connection edn)]
            {:db (result :db-after) :result [:ok id (serialize-transaction-response result)]})
      [:migrate id migration-path] 
          (let [result (migrate connection migration-path)]
            {:db (result :db-after) :result [:ok id :migrated]})
      [:load id data-resource-path] 
          (let [result (load-data db-url connection data-resource-path)]
            {:db (result :db-after) :result [:ok id (serialize-transaction-response result)]})
      [:ping] {:db database :result [:ok :ping]}
      [:stop] (do (datomic/shutdown false) nil) ; For testing from Clojure; does not release Clojure resources
      [:exit] (do (datomic/shutdown true) nil) ; Shuts down Clojure resources as part of JVM shutdown
      nil (do (datomic/shutdown true) nil)) ; Handle close of STDIN - parent is gone
    (catch Exception e 
      (let [response {:db database :result [:error message e]}]
        response))))

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
                result (process-message message database connection db-url)]
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
        
; TODO:
; 1. When we send the :mock message, we:
;   a. Check the environment to see if mocking is enabled
;   b. If mocking is not enabled, return the current db, the map, the real connection, and the real connection as the active one
;   c. Otherwise, we create a new mock connection with the current db value
;   d. Save the db value as a "starting-point" db in a map of dbs with the key passed by mock
;   e. Return the db, the map of db values, the real connection, and the mocked connection as the active one
;   
; 2. When we send a new message we continue to use the current db and connection,
;     but have to pass those same 4 things out of the loop.
; 
; 3. When we send a :reset message, we
;   a. Check the environment to see if mocking is enabled
;   b. If mocking is not enabled, return the current db, the map, the real connection, and the real connection as the active one
;   c. Use the key in the :reset message to get the db we are resetting to, with a special key for "live"
;   d. If we are resetting to the live db we use the real connection and get the db from it.
;      Otherwise, we get the db using the key, and create a new mocked connection from it.
