(defproject datomic_gen_server/peer "2.0.0"
  :description "Datomic peer server in Clojure, accepting edn strings which will be sent by Elixir"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [clojure-erlastic "0.3.0"]
                 [com.datomic/datomic-pro "0.9.5344"]
                 [vvvvalvalval/datomock "0.1.0"]
                 [net.phobot.datomic/seed "3.0.0"]
                ]
  :main datomic_gen_server.peer
  :aot :all)
