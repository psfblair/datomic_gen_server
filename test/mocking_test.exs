defmodule MockingTest do
  use ExUnit.Case, async: false
  
  setup_all do
    Application.put_env(:datomic_gen_server, :allow_datomic_mocking?, true)
    # Need long timeouts to let the JVM start.
    DatomicGenServer.start_link(
      "datomic:mem://mocking-test", 
      true, 
      [{:timeout, 20_000}, {:default_message_timeout, 20_000}, {:name, MockingTestServer}]
    )
    :ok
  end
  
  test "Can mock a database" do
    migration_dir = Path.join [System.cwd(), "priv", "datomic_gen_server_peer", "test", "resources", "migrations" ]
    {:ok, :migrated} = DatomicGenServer.migrate(MockingTestServer, migration_dir)
    
    query = "[:find ?c :where [?e :db/doc \"A category's name\"] [?e :db/ident ?c]]"
    {:ok, result_str} = DatomicGenServer.q(MockingTestServer, query)
    assert "\#{[:category/name]}\n" == result_str
    
    query = "[:find ?e :where [?e :category/name \"Sports\"]]"
    {:ok, result_str} = DatomicGenServer.q(MockingTestServer, query)
    assert "\#{}\n" == result_str

    {:ok, :"just-migrated"} = DatomicGenServer.mock(MockingTestServer, :"just-migrated")
    
    data_dir = Path.join [System.cwd(), "priv", "datomic_gen_server_peer", "test", "resources", "seed" ]
    {:ok, _} = DatomicGenServer.load(MockingTestServer, data_dir)
    
    query = "[:find ?e :where [?e :category/name \"Sports\"]]"
    {:ok, result_str} = DatomicGenServer.q(MockingTestServer, query)
    assert Regex.match?(~r/\#\{\[\d+\]\}\n/, result_str)
    
    {:ok, :seeded} = DatomicGenServer.mock(MockingTestServer, :seeded)
        
    {:ok, :"just-migrated"} = DatomicGenServer.reset(MockingTestServer, :"just-migrated")
  
    query = "[:find ?e :where [?e :category/name \"Sports\"]]"
    {:ok, result_str} = DatomicGenServer.q(MockingTestServer, query)
    assert "\#{}\n" == result_str
    
    {:ok, :unmocked} = DatomicGenServer.unmock(MockingTestServer)

    data_to_add = "[ { :db/id #db/id[:test/main] :category/name \"News\"} ]" 
    {:ok, _} = DatomicGenServer.transact(MockingTestServer, data_to_add)
    
    query = "[:find ?e :where [?e :category/name \"Sports\"]]"
    {:ok, result_str} = DatomicGenServer.q(MockingTestServer, query)
    assert "\#{}\n" == result_str
    
    query = "[:find ?e :where [?e :category/name \"News\"]]"
    {:ok, result_str} = DatomicGenServer.q(MockingTestServer, query)
    assert Regex.match?(~r/\#\{\[\d+\]\}\n/, result_str)
    
    {:ok, :"just-migrated"} = DatomicGenServer.reset(MockingTestServer, :"just-migrated")

    query = "[:find ?e :where [?e :category/name \"Sports\"]]"
    {:ok, result_str} = DatomicGenServer.q(MockingTestServer, query)
    assert "\#{}\n" == result_str
    
    query = "[:find ?e :where [?e :category/name \"News\"]]"
    {:ok, result_str} = DatomicGenServer.q(MockingTestServer, query)
    assert "\#{}\n" == result_str
    
    {:ok, :seeded} = DatomicGenServer.reset(MockingTestServer, :seeded)

    query = "[:find ?e :where [?e :category/name \"Sports\"]]"
    {:ok, result_str} = DatomicGenServer.q(MockingTestServer, query)
    assert Regex.match?(~r/\#\{\[\d+\]\}\n/, result_str)
    
    query = "[:find ?e :where [?e :category/name \"News\"]]"
    {:ok, result_str} = DatomicGenServer.q(MockingTestServer, query)
    assert "\#{}\n" == result_str
    
    Application.put_env(:datomic_gen_server, :allow_datomic_mocking, false)
  end
end
