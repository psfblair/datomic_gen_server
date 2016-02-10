defmodule DatomicGenServerTest do
  use ExUnit.Case, async: false
  
  setup_all do
    # Need a long timeout to let the JVM start.
    DatomicGenServer.start_link("datomic:mem://test", true, 20_000)
    :ok
  end
  
  test "can issue Datomic queries" do
    query = "[:find ?c :where [?c :db/doc \"Some docstring that shouldn't be in the database\"]]"
    result = DatomicGenServer.q(query)
    assert {:ok, "\#{}\n"} == result
    second_result = DatomicGenServer.q(query)
    assert {:ok, "\#{}\n"} == second_result
  end
  
  test "can execute Datomic transactions" do
    data_to_add = """
      [ { :db/id #db/id[:db.part/db]
          :db/ident :person/name
          :db/valueType :db.type/string
          :db/cardinality :db.cardinality/one
          :db/doc \"A person's name\"
          :db.install/_attribute :db.part/db}]
    """
    {:ok, transaction_result} = DatomicGenServer.transact(data_to_add)
    assert Regex.match?(~r/:db-before \{:basis-t \d+/, transaction_result)
    assert Regex.match?(~r/:db-after \{:basis-t \d+/, transaction_result)
    assert Regex.match?(~r/:tx-data \[\{:a \d+/, transaction_result)
    assert Regex.match?(~r/:tempids \{/, transaction_result)

    query = "[:find ?c :where [?c :db/doc \"A person's name\"]]"
    {:ok, result_str} = DatomicGenServer.q(query)
    assert Regex.match?(~r/\#\{\[\d+\]\}\n/, result_str)
  end
  
  test "Handles garbled queries" do
    query = "[:find ?c :wh?ere]"
    {:error, query_result} = DatomicGenServer.q(query)
    assert Regex.match?(~r/Exception/, query_result)
  end
  
  test "Handles garbled transactions" do
    data_to_add = """
      [ { :db/ii #db/foo[:db.part/db]
    """
    {:error, transaction_result} = DatomicGenServer.transact(data_to_add)
    assert Regex.match?(~r/Exception/, transaction_result)
  end
end
