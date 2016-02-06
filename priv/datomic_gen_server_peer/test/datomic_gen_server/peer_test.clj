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
  (testing "Can handle unknown messages"
    (>!! in [:ping])
    (is (= {:ok "#{}\n"} (<!! out)))))

(deftest test-round-trip
  (testing "Can transact and query data"
  
    (>!! in [:q "[:find ?c :where [?c :db/doc \"A person's name\"]]"])
    (is (= {:ok "#{}\n"} (<!! out)))
    
    (>!! in [:transact "[ {:db/id #db/id[:db.part/db]
                           :db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/doc \"A person's name\"
                           :db.install/_attribute :db.part/db}]"])
    (let [ignore-edn-tags {:default #(identity [%1 %2])}
          response-edn (:ok (<!! out))
          edn-data (clojure.edn/read-string ignore-edn-tags response-edn)]
      (is (= 6 (count edn-data))))
      
    (>!! in [:q "[:find ?c :where [?c :db/doc \"A person's name\"]]"])
    (is (not (= {:ok "#{}\n"} (<!! out))))))

(deftest test-unknown-messages
  (testing "Can handle unknown messages"
    (>!! in [:unknown "[:find ?c :where [?c :db/doc \"A person's name\"]]"])
    (is (contains? (<!! out) :error))))
      
(deftest test-garbled-messages
  (testing "Can handle garbled messages"
    (>!! in [:query "[:find ?c }"])
    (is (contains? (<!! out) :error))))
