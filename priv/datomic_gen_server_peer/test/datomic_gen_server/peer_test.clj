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
        (datomic/delete-database db-uri)
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

(deftest test-mocking
  (testing "Can use a mock connection on a just-migrated database"
    (let [migration-dir (clojure.java.io/file (System/getProperty "user.dir") 
                                                "test" "resources" "migrations")]
      (>!! in [:migrate 18 (.getPath migration-dir)]))
    (<!! out)
  
  ; (let [seed-dir (clojure.java.io/file (System/getProperty "user.dir") "test" "resources" "seed")]
  ;   (>!! in [:load 11 (.getPath seed-dir)]))
  ; (<!! out) 
    ; Test mock
    (System/setProperty "datomic.mocking" "true")
    
    (>!! in [:mock 19 :freshly-migrated])
    (is (= [:ok 19 :freshly-migrated] (<!! out)))

    (>!! in [:q 20 "[:find ?c :where [?e :db/doc \"A category's name\"] [?e :db/ident ?c]]" '()])
    (is (= [:ok 20 "#{[:category/name]}\n"] (<!! out)))

    (>!! in [:q 21 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (is (= [:ok 21 "#{}\n"] (<!! out)))
    
    (>!! in [:transact 22 "[ { :db/id #db/id[:test/main]
                               :category/name \"Sports\"}]"])
    (<!! out)
    
    (>!! in [:q 23 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (let [query-result (read-edn-response (<!! out))]
      (is (= 1 (count query-result))))

    ; Test reset
    (>!! in [:reset 24 :freshly-migrated])
    (is (= [:ok 24 :freshly-migrated] (<!! out)))

    (>!! in [:q 25 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (is (= [:ok 25 "#{}\n"] (<!! out)))

    (>!! in [:transact 26 "[ { :db/id #db/id[:test/main]
                               :category/name \"Sports\"}]"])
    (<!! out)
    
    (>!! in [:q 27 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (let [query-result (read-edn-response (<!! out))]
      (is (= 1 (count query-result))))

    ; Test unmock
    (>!! in [:unmock 28])
    (is (= [:ok 28] (<!! out)))
    
    (>!! in [:q 29 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (is (= [:ok 29 "#{}\n"] (<!! out)))
    
    (>!! in [:transact 30 "[ { :db/id #db/id[:test/main]
                               :category/name \"Sports\"}]"])
    (<!! out)
    
    (>!! in [:q 31 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (let [query-result (read-edn-response (<!! out))]
      (is (= 1 (count query-result))))
    
    ; Make sure mock starts with new db state, but doesn't change saved or active state
    (>!! in [:mock 32 :with-sports])
    (is (= [:ok 32 :with-sports] (<!! out)))
    
    (>!! in [:q 33 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (let [query-result (read-edn-response (<!! out))]
      (is (= 1 (count query-result))))
      
    (>!! in [:q 34 (str "[:find ?e :where [?e :category/name \"News\"]]") '()])
    (is (= [:ok 34 "#{}\n"] (<!! out)))
      
    (>!! in [:transact 35 "[ { :db/id #db/id[:test/main]
                               :category/name \"News\"}]"])
    (<!! out)
    
    (>!! in [:q 36 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (let [query-result (read-edn-response (<!! out))]
      (is (= 1 (count query-result))))
      
    (>!! in [:q 37 (str "[:find ?e :where [?e :category/name \"News\"]]") '()])
    (let [query-result (read-edn-response (<!! out))]
      (is (= 1 (count query-result))))
    
    ; Go back to freshly-migrated db with neither Sports nor News
    (>!! in [:reset 38 :freshly-migrated])
    (is (= [:ok 38 :freshly-migrated] (<!! out)))

    (>!! in [:q 39 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (is (= [:ok 39 "#{}\n"] (<!! out)))
      
    (>!! in [:q 40 (str "[:find ?e :where [?e :category/name \"News\"]]") '()])
    (is (= [:ok 40 "#{}\n"] (<!! out)))
    
    ; Go back to active db/connection with Sports but not News
    (>!! in [:unmock 41])
    (is (= [:ok 41] (<!! out)))
    
    (>!! in [:q 42 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (let [query-result (read-edn-response (<!! out))]
      (is (= 1 (count query-result))))
      
    (>!! in [:q 43 (str "[:find ?e :where [?e :category/name \"News\"]]") '()])
    (is (= [:ok 43 "#{}\n"] (<!! out)))
    
    (System/setProperty "datomic.mocking" "false")
  ))

  (deftest test-mocking-system-property
    (testing "Mock connections don't work if System property is not set"
      (let [migration-dir (clojure.java.io/file (System/getProperty "user.dir") 
                                                  "test" "resources" "migrations")]
        (>!! in [:migrate 44 (.getPath migration-dir)]))
      (<!! out)

      (>!! in [:mock 45 :freshly-migrated])
      (is (= [:ok 45 :freshly-migrated] (<!! out)))

      (>!! in [:q 46 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
      (is (= [:ok 46 "#{}\n"] (<!! out)))
      
      (>!! in [:transact 47 "[ { :db/id #db/id[:test/main]
                                 :category/name \"Sports\"}]"])
      (<!! out)
      
      (>!! in [:q 48 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
      (let [query-result (read-edn-response (<!! out))]
        (is (= 1 (count query-result))))

      (>!! in [:reset 49 :freshly-migrated])
      (is (= [:ok 49 :freshly-migrated] (<!! out)))

      (>!! in [:q 50 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
      (let [query-result (read-edn-response (<!! out))]
        (is (= 1 (count query-result))))

      (>!! in [:unmock 51])
      (is (= [:ok 51] (<!! out)))
      
      (>!! in [:q 52 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
      (let [query-result (read-edn-response (<!! out))]
        (is (= 1 (count query-result))))
    ))
    
(deftest test-mocking-seeded
  (testing "Can use a mock connection on both a just-migrated and just-seeded database"
    (let [migration-dir (clojure.java.io/file (System/getProperty "user.dir") 
                                                "test" "resources" "migrations")]
      (>!! in [:migrate 53 (.getPath migration-dir)]))
    (<!! out)
    
    (System/setProperty "datomic.mocking" "true")
    (>!! in [:mock 54 :freshly-migrated])
    (is (= [:ok 54 :freshly-migrated] (<!! out)))
    
    (>!! in [:q 55 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (is (= [:ok 55 "#{}\n"] (<!! out)))

    (>!! in [:unmock 56])
    (is (= [:ok 56] (<!! out)))

    (let [seed-dir (clojure.java.io/file (System/getProperty "user.dir") "test" "resources" "seed")]
      (>!! in [:load 57 (.getPath seed-dir)]))
    (<!! out) 
      
    (>!! in [:mock 58 :freshly-seeded])
    (is (= [:ok 58 :freshly-seeded] (<!! out)))
    
    (>!! in [:q 59 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (let [query-result (read-edn-response (<!! out))]
      (is (= 1 (count query-result))))
    
    (>!! in [:reset 60 :freshly-migrated])
    (is (= [:ok 60 :freshly-migrated] (<!! out)))

    (>!! in [:q 61 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (is (= [:ok 61 "#{}\n"] (<!! out)))
    
    (>!! in [:reset 62 :freshly-seeded])
    (is (= [:ok 62 :freshly-seeded] (<!! out)))
    
    (>!! in [:q 63 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (let [query-result (read-edn-response (<!! out))]
      (is (= 1 (count query-result))))

    (>!! in [:unmock 64])
    (is (= [:ok 64] (<!! out)))
    
    (>!! in [:q 65 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (let [query-result (read-edn-response (<!! out))]
      (is (= 1 (count query-result))))
    
    (>!! in [:transact 66 "[ { :db/id #db/id[:test/main]
                               :category/name \"News\"}]"])
    (<!! out)
    
    (>!! in [:q 67 (str "[:find ?e :where [?e :category/name \"News\"]]") '()])
    (let [query-result (read-edn-response (<!! out))]
      (is (= 1 (count query-result))))
      
    (>!! in [:reset 68 :freshly-migrated])
    (is (= [:ok 68 :freshly-migrated] (<!! out)))

    (>!! in [:q 69 (str "[:find ?e :where [?e :category/name \"News\"]]") '()])
    (is (= [:ok 69 "#{}\n"] (<!! out)))
    
    (>!! in [:q 70 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (is (= [:ok 70 "#{}\n"] (<!! out)))
    
    (>!! in [:reset 71 :freshly-seeded])
    (is (= [:ok 71 :freshly-seeded] (<!! out)))
    
    (>!! in [:q 72 (str "[:find ?e :where [?e :category/name \"News\"]]") '()])
    (is (= [:ok 72 "#{}\n"] (<!! out)))
    
    (>!! in [:q 73 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (let [query-result (read-edn-response (<!! out))]
      (is (= 1 (count query-result))))
        
    (>!! in [:unmock 74])
    (is (= [:ok 74] (<!! out)))
    
    (>!! in [:q 75 (str "[:find ?e :where [?e :category/name \"Sports\"]]") '()])
    (let [query-result (read-edn-response (<!! out))]
      (is (= 1 (count query-result))))
    
    (>!! in [:q 76 (str "[:find ?e :where [?e :category/name \"News\"]]") '()])
    (let [query-result (read-edn-response (<!! out))]
      (is (= 1 (count query-result))))
    
    (System/setProperty "datomic.mocking" "false")
  ))
