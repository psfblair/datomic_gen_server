(ns datomic_gen_server.peer_test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [>!! <!! go chan close!]]
            [datomic.api :as datomic]
            [datomic_gen_server.peer :refer :all]))

(def ^:dynamic in nil)
(def ^:dynamic out nil)

(defn read-edn-response [response]
  (let [response-edn (nth response 2)]
    (clojure.edn/read-string *data-readers* response-edn)))

;; NOTE If you send a message and don't read the reply from `out`
;; the test is going to hang!!!!
(defn db-fixture [test-fun]
  (let [db-uri "datomic:mem://test"]
    (binding [in (chan) out (chan)]
      (let [exit-channel (go (start-server db-uri in out true))]
        (test-fun)
        (>!! in [:stop]) ; This does a datomic/shutdown but does not release clojure resources
        (<!! exit-channel) ; Make sure we've exited before going on
        (close! in)
        (close! out)
        (close! exit-channel)
      ))))
      
(use-fixtures :each db-fixture)

(deftest test-handles-ping
  (testing "Can handle a ping message"
    (>!! in [:ping])
    (is (= [:ok :ping] (<!! out)))))

(deftest test-round-trip
  (testing "Can query and transact data"
  
    (>!! in [:q 1 "[:find ?c :where [?c :db/doc \"A person's name\"]]" '()])
    (is (= [:ok 1 "#{}\n"] (<!! out)))
    
    (>!! in [:transact 2 "[ {:db/id #db/id[:db.part/db]
                           :db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/doc \"A person's name\"
                           :db.install/_attribute :db.part/db}]"])
    (let [response (<!! out)
          edn-data (read-edn-response response)]
      (is (= (response 0) :ok))
      (is (= (response 1) 2))  
      (is (= java.lang.Long (type ((edn-data :db-before) :basis-t))))
      ; TODO Can we get this somehow?
      ; (is (= "test" ((edn-data :db-before) :db/alias)))
      (is (= java.lang.Long (type ((edn-data :db-after) :basis-t))))
      ; TODO Can we get this somehow?
      ; (is (= "test" ((edn-data :db-after) :db/alias)))
      (is (> ((edn-data :db-after) :basis-t) ((edn-data :db-before) :basis-t)))
      
      (is (= 6 (count (edn-data :tx-data))))
      (is (= java.lang.Long (type ((nth (edn-data :tx-data) 0) :e))))
      (is (= java.lang.Long (type ((nth (edn-data :tx-data) 0) :a))))
      (is (contains? (nth (edn-data :tx-data) 0) :v))
      (is (= java.lang.Long (type ((nth (edn-data :tx-data) 0) :tx))))
      (is (= true ((nth (edn-data :tx-data) 0) :added)))
      (is (= clojure.lang.PersistentArrayMap (type (edn-data :tempids))))
      )
      
    (>!! in [:q 3 "[:find ?c :where [?c :db/doc \"A person's name\"]]" '()])
    (let [query-result (<!! out)]
      (is (= (query-result 0) :ok))
      (is (= (query-result 1) 3))
      (is (not (= "#{}\n" (query-result 2)))))))

;; Finds an entity id for a datom with a certain value
(defn- entity-id-for-value [tx-data value]
  (let [datom (some #(if (= (% :v) value) %) tx-data)]
    (datom :e)))
  
(deftest test-entity
  (testing "Can ask for an entity"
    (>!! in [:transact 4 "[ {:db/id #db/id[:db.part/db]
                           :db/ident :person/email
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/doc \"A person's email\"
                           :db.install/_attribute :db.part/db}]"])
    (let [edn-data (read-edn-response (<!! out))
          ; We get different entities back in the transaction response. Get the email one
          entity-id (entity-id-for-value (edn-data :tx-data) :person/email)]
      (>!! in [:entity 5 (str entity-id) :all])
      (let [response (<!! out)
            response-edn (read-edn-response response)
            expected {:db/ident :person/email
                      :db/valueType :db.type/string
                      :db/cardinality :db.cardinality/one
                      :db/doc "A person's email"}]
        (is (= (response 0) :ok))
        (is (= (response 1) 5))
        (is (= expected response-edn))))
    
    ;; Look up using ident; second item in tuple is an edn string
    (>!! in [:entity 6 (str :person/email) [:db/valueType :db/doc]])
    (let [response-edn (read-edn-response (<!! out))]
      (is (= {:db/valueType :db.type/string :db/doc "A person's email"} response-edn)))
    
    ; Lookup using lookup ref; attribute must be unique so we can't use :db/doc
    (>!! in [:entity 7 (str [:db/ident :person/email]) [:db/ident]])
    (let [response-edn (read-edn-response (<!! out))]
      (is (= {:db/ident :person/email} response-edn)))
    ))

(deftest test-migration
  (testing "Can run data migrations"
    (let [migration-dir (clojure.java.io/file (System/getProperty "user.dir") 
                                              "test" "resources" "migrations")]
      (>!! in [:migrate 8 (.getPath migration-dir)]))
    (is (= [:ok 8 :migrated] (<!! out)))
    (>!! in [:q 9 "[:find ?c :where [?e :db/doc \"A category's name\"] [?e :db/ident ?c]]" '()])
    (let [query-result (<!! out)]
      (is (= (query-result 0) :ok))
      (is (= (query-result 1) 9))
      (is (= "#{[:category/name]}\n" (query-result 2))))))
  
(deftest test-seed
  (testing "Can seed a database"
    (let [migration-dir (clojure.java.io/file (System/getProperty "user.dir") 
                                                "test" "resources" "migrations")]
      (>!! in [:migrate 10 (.getPath migration-dir)]))
    (is (= [:ok 10 :migrated] (<!! out)))
    
    (let [seed-dir (clojure.java.io/file (System/getProperty "user.dir") "test" "resources" "seed")]
      (>!! in [:load 11 (.getPath seed-dir)]))
    (let [transaction-result (<!! out)]
        (is (= (transaction-result 0) :ok))
        (is (= (transaction-result 1) 11))
        (is (.startsWith (transaction-result 2) "{:db-before {:basis-t")))
    (>!! in [:q 12 (str "[:find ?c :where "
                        "[?e :category/name ?c] "
                        "[?e :category/subcategories ?s] "
                        "[?s :subcategory/name \"Soccer\"]]") 
              '()])
    (let [query-result (<!! out)]
      (is (= (query-result 0) :ok))
      (is (= (query-result 1) 12))
      (is (= "#{[\"Sports\"]}\n" (query-result 2))))))
  
(deftest test-unknown-messages
  (testing "Can handle unknown messages"
    (>!! in [:unknown 13 "[:find ?c :where [?c :db/doc \"A person's name\"]]"])
    (let [response (<!! out)]
      (is (= (nth response 0) :error))
      (is (= (nth response 1) [:unknown 13 "[:find ?c :where [?c :db/doc \"A person's name\"]]"])))))
      
(deftest test-garbled-messages
  (testing "Can handle garbled messages"
    (>!! in [:q 14 "[:find ?c }" '()])
    (let [response (<!! out)]
      (is (= (nth response 0) :error))
      (is (= (nth response 1) [:q 14 "[:find ?c }" '()])))))

(deftest test-query-bindings
  (testing "Can bind queries"
    (>!! in [:transact 15 "[ {:db/id #db/id[:db.part/db]
                             :db/ident :person/address
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one
                             :db/doc \"A person's address\"
                             :db.install/_attribute :db.part/db}]"])
                             
     (let [edn-data (read-edn-response (<!! out))
           basis-t-before ((edn-data :db-before) :basis-t)
           basis-t-after ((edn-data :db-after) :basis-t)
           as-of-before (str "(datomic.api/as-of datomic_gen_server.peer/*db* " basis-t-before ")")
           as-of-after (str "(datomic.api/as-of datomic_gen_server.peer/*db* " basis-t-after ")")
           query "[:find ?ident :in $ ?docstring :where [?e :db/doc ?docstring][?e :db/ident ?ident]]"
           before-bindings (list as-of-before "\"A person's address\"")
           after-bindings (list as-of-after "\"A person's address\"")]
           
         (>!! in [:q 16 query before-bindings])
     
         (let [query-result (read-edn-response (<!! out))]
           (is (= #{} query-result)))
       
         (>!! in [:q 17 query after-bindings])
     
         (let [query-result (read-edn-response (<!! out))]
           (is (= #{[:person/address]} query-result))))))

;; TODO - Change to test use of mock connections. :with is no longer a valid message.
; (deftest test-with-with
;   (testing "Can use a mock connection on a just-migrated database"
;     (let [migration-dir (clojure.java.io/file (System/getProperty "user.dir") 
;                                                 "test" "resources" "migrations")]
;       (>!! in [:seed 10 (.getPath migration-dir) (.getPath seed-dir)]))
; 
;     (>!! in [:transact 15 "[ { :db/id #db/id[:db.part/db]
;                                :db/ident :business/address
;                                :db/valueType :db.type/string
;                                :db/cardinality :db.cardinality/one
;                                :db/doc \"A business's address\"
;                                :db.install/_attribute :db.part/db}]"])
;     (<!! out)
;       
;     (>!! in [:transact 16 "[ { :db/id #db/id[:db.part/user]
;                                :business/address \"222 Main Timeline St.\"}]"])
;     (let [init-data (read-edn-response (<!! out))
;           basis-t-before ((init-data :db-before) :basis-t)
;           basis-t-after ((init-data :db-after) :basis-t)
;           as-of-before (str "(datomic.api/as-of datomic_gen_server.peer/*db* " basis-t-before ")")
;           as-of-after (str "(datomic.api/as-of datomic_gen_server.peer/*db* " basis-t-after ")")
;           ]
;       (>!! in [:with 17 "[ { :db/id #db/id[:db.part/user]
;                               :business/address \"1980 Speculative Road\"}]" as-of-before])
;       (let [ speculative-response (<!! out)
;              speculative-data (read-edn-response speculative-response)
;              ; NOTE: basis-t is not useful here. The basis-t before is still the latest
;              ; t that could be reached from *db* before the speculative transaction,
;              ; which is the db-after of the previous transaction, even though the
;              ; speculative transaction was against the basis-t-before of the previous
;              ; transaction. In addition, the db-after returned by `when` does not 
;              ; differ from the db-before value -- they are both still the latest t
;              ; that could be reached from *db* and does not include the speculative transaction.
;              speculative-tx-id ((first (speculative-data :tx-data)) :tx)
;              as-of-speculative 
;                 (str "(datomic.api/as-of datomic_gen_server.peer/*db* " speculative-tx-id ")")
;              query "[:find ?e :in $ ?address :where [?e :business/address ?address]]"]
; 
;         (is (= basis-t-before speculative-basis-t-before))
;         
;         (>!! in [:q 18 query (list as-of-speculative "\"222 Main Timeline St.\"")])
;         
;         (is (= #{} (read-edn-response (<!! out))))
;         
;         (>!! in [:q 19 query (list as-of-speculative "\"1980 Speculative Road\"")])
;         
;         (let [entity-id (first (first (read-edn-response (<!! out))))]
;           (is (< 0 entity-id)))
;           
;         (>!! in [:q 20 query (list as-of-after "\"222 Main Timeline St.\"")])
;           
;         (let [entity-id (first (first (read-edn-response (<!! out))))]
;           (is (< 0 entity-id)))
;           
;         (>!! in [:q 21 query (list as-of-after "\"1980 Speculative Road\"")])
;           
;         (is (= #{} (read-edn-response (<!! out))))
;       ))))
