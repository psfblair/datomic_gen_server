defmodule DbSeedDataTest do
  use ExUnit.Case, async: false
  alias DatomicGenServer.Db, as: Db
  
  setup_all do
    # Need long timeouts to let the JVM start.
    DatomicGenServer.start_link(
      "datomic:mem://db-seed-data-test", 
      true, 
      [{:timeout, 20_000}, {:default_message_timeout, 20_000}, {:name, LoadDataServer}]
    )
    :ok
  end
  
  test "Can seed a database" do
    migration_dir = Path.join [System.cwd(), "priv", "datomic_gen_server_peer", "test", "resources", "migrations" ]
    {:ok, :migrated} = DatomicGenServer.migrate(LoadDataServer, migration_dir)
    data_dir = Path.join [System.cwd(), "priv", "datomic_gen_server_peer", "test", "resources", "seed" ]
    {:ok, transaction_result} = Db.load(LoadDataServer, data_dir)
    
    assert is_integer(transaction_result.basis_t_before)
    assert is_integer(transaction_result.basis_t_after)
    assert (transaction_result.basis_t_after - transaction_result.basis_t_before) > 0
    
    retracted_datoms = transaction_result.retracted_datoms
    assert 0 == Enum.count(retracted_datoms)
    
    added_datoms = transaction_result.added_datoms
    assert 16 == Enum.count(added_datoms)
    
    first_datom = hd(added_datoms)
    assert is_integer(first_datom.e)
    assert is_integer(first_datom.a)
    assert ! is_nil(first_datom.v)
    assert is_integer(first_datom.tx)
    assert first_datom.added
    
    tempids = transaction_result.tempids
    assert 0 == Enum.count(tempids)
  end
end
