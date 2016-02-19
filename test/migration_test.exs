defmodule MigrationTest do
  use ExUnit.Case, async: false
  
  setup_all do
    # Need long timeouts to let the JVM start.
    DatomicGenServer.start_link(
      "datomic:mem://migration-test", 
      true, 
      [{:timeout, 20_000}, {:default_message_timeout, 20_000}, {:name, MigrationTestServer}]
    )
    :ok
  end
  
  test "Can migrate a database" do
    migration_dir = Path.join [System.cwd(), "priv", "datomic_gen_server_peer", "test", "resources", "migrations" ]
    {:ok, :migrated} = DatomicGenServer.migrate(MigrationTestServer, migration_dir)
    query = "[:find ?c :where [?e :db/doc \"A category's name\"] [?e :db/ident ?c]]"
    {:ok, result_str} = DatomicGenServer.q(MigrationTestServer, query)
    assert "\#{[:category/name]}\n" == result_str
  end
end
