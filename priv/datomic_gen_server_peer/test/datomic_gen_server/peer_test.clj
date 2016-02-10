(ns datomic_gen_server.peer_test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [>!! <!! go chan close!]]
            [datomic.api :as datomic]
            [datomic_gen_server.peer :refer :all]))

(def ^:dynamic in nil)
(def ^:dynamic out nil)

;; NOTE If you send a message and don't read the reply from `out`
;; the test is going to hang!!!!
(defn db-fixture [test-fun]
  (let [db-uri "datomic:mem://test"]
    (binding [in (chan) out (chan)]
      (let [exit-channel (go (start-server db-uri in out true))]
        (test-fun)
        (>!! in [:exit])
        (<!! exit-channel) ; Make sure we've exited before going on
        (close! in)
        (close! out)
        (close! exit-channel)
      ))))
      
(use-fixtures :each db-fixture)

(deftest test-handles-ping
  (testing "Can handle a ping message"
    (>!! in [:ping])
    (is (= [:ok "#{}\n"] (<!! out)))))

(deftest test-round-trip
  (testing "Can query and transact data"
  
    (>!! in [:q "[:find ?c :where [?c :db/doc \"A person's name\"]]"])
    (is (= [:ok "#{}\n"] (<!! out)))
    
    (>!! in [:transact "[ {:db/id #db/id[:db.part/db]
                           :db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/doc \"A person's name\"
                           :db.install/_attribute :db.part/db}]"])
    (let [ignore-edn-tags {:default #(identity [%1 %2])}
          response-edn (nth (<!! out) 1)
          edn-data (clojure.edn/read-string ignore-edn-tags response-edn)]
      (is (= java.lang.Long (type ((edn-data :db-before) :basis-t))))
      ; TODO Can we get this somehow?
      ; (is (= "test" ((edn-data :db-before) :db/alias)))
      (is (= java.lang.Long (type ((edn-data :db-after) :basis-t))))
      ; TODO Can we get this somehow?
      ; (is (= "test" ((edn-data :db-after) :db/alias)))
      (is (= 6 (count (edn-data :tx-data))))
      (is (= java.lang.Long (type ((nth (edn-data :tx-data) 0) :e))))
      (is (= java.lang.Long (type ((nth (edn-data :tx-data) 0) :a))))
      (is (contains? (nth (edn-data :tx-data) 0) :v))
      (is (= java.lang.Long (type ((nth (edn-data :tx-data) 0) :tx))))
      (is (= true ((nth (edn-data :tx-data) 0) :added))))
      
    (>!! in [:q "[:find ?c :where [?c :db/doc \"A person's name\"]]"])
    (let [query-result (<!! out)]
      (is (= (query-result 0) :ok))
      (is (not (= "#{}\n" (query-result 1)))))))

(deftest test-unknown-messages
  (testing "Can handle unknown messages"
    (>!! in [:unknown "[:find ?c :where [?c :db/doc \"A person's name\"]]"])
    (is (= (nth (<!! out) 0) :error))))
      
(deftest test-garbled-messages
  (testing "Can handle garbled messages"
    (>!! in [:query "[:find ?c }"])
    (is (= (nth (<!! out) 0) :error))))
