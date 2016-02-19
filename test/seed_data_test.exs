defmodule SeedDataTest do
  use ExUnit.Case, async: false
  
  setup_all do
    # Need long timeouts to let the JVM start.
    DatomicGenServer.start_link(
      "datomic:mem://seed-data-test", 
      true, 
      [{:timeout, 20_000}, {:default_message_timeout, 20_000}, {:name, SeedDataServer}]
    )
    :ok
  end
  
  test "Can seed a database" do
    migration_dir = Path.join [System.cwd(), "priv", "datomic_gen_server_peer", "test", "resources", "migrations" ]
    seed_dir = Path.join [System.cwd(), "priv", "datomic_gen_server_peer", "test", "resources", "seed" ]
    {:ok, _} = DatomicGenServer.seed(SeedDataServer, migration_dir, seed_dir)
    
    query = "[:find ?c :where " <>
            "[?e :category/name ?c] " <>
            "[?e :category/subcategories ?s] " <>
            "[?s :subcategory/name \"Soccer\"]]"
    {:ok, result_str} = DatomicGenServer.q(SeedDataServer, query)
    assert "\#{[\"Sports\"]}\n" == result_str            
  end
end
