defmodule DatomicGenServer.DbTest do
  use ExUnit.Case, async: false
  alias DatomicGenServer.Db, as: Db
  
  setup_all do
    # Need a long timeout to let the JVM start.
    DatomicGenServer.start_link("datomic:mem://test", true, [{:timeout, 20_000}])
    :ok
  end
  
  test "can issue Datomic queries" do
    query = [:find, Db.q?(:c), :where, [Db.q?(:c), :"db/doc", "Some docstring that shouldn't be in the database"]]
    result = Db.q(query)
    
    empty_set = MapSet.new()
    assert {:ok, empty_set} == result

    second_result = Db.q(query)
    assert {:ok, empty_set} == second_result
  end
  
  test "can execute Datomic transactions" do
    data_to_add = [%{ 
        Db.id => Db.dbid(Db.schema_partition),
        Db.ident => :"person/name",
        Db.value_type => Db.type_string,
        Db.cardinality => Db.cardinality_one,
        Db.doc => "A person's name",
        Db.install_attribute => Db.schema_partition
    }]
    
    {:ok, transaction_result} = Db.transact(data_to_add)
    before_t = Db.basis_t_before(transaction_result)
    assert is_integer(before_t)
    
    after_t = Db.basis_t_after(transaction_result)
    assert is_integer(after_t)
    
    tx_data = Db.tx_data(transaction_result)
    assert 6 == Enum.count(tx_data)
    %{e: entity} = hd(tx_data)
    assert is_integer(entity)
    %{a: attribute} = hd(tx_data)
    assert is_integer(attribute)
    %{v: _} = hd(tx_data)
    %{tx: transaction} = hd(tx_data)
    assert is_integer(transaction)
    %{added: added?} = hd(tx_data)
    assert added?
    
    tempids = Db.tempids(transaction_result)
    assert 1 == Enum.count(tempids)
    assert (Map.keys(tempids) |> hd |> is_integer)
    assert (Map.values(tempids) |> hd |> is_integer)

    query = [:find, Db.q?(:c), :where, [Db.q?(:c), :"db/doc", "A person's name"]]
    {:ok, query_result} = Db.q(query)
    assert 1 == Enum.count(query_result)
  end
  
  test "can ask for an entity" do
    data_to_add = [%{ 
        Db.id => Db.dbid(Db.schema_partition),
        Db.ident => :"person/email",
        Db.value_type => Db.type_string,
        Db.cardinality => Db.cardinality_one,
        Db.doc => "A person's email",
        Db.install_attribute => Db.schema_partition
    }]
    {:ok, _} = Db.transact(data_to_add)

    all_attributes = 
      %{ Db.ident => :"person/email", 
         Db.value_type => Db.type_string, 
         Db.cardinality => Db.cardinality_one, 
         Db.doc => "A person's email"
        }
      
    {:ok, entity_result} = Db.entity(:"person/email")
    assert all_attributes == entity_result

    {:ok, entity_result2} = Db.entity(:"person/email", :all)
    assert all_attributes == entity_result2

    {:ok, entity_result3} = Db.entity(:"person/email", [Db.value_type, Db.doc])
    assert %{Db.value_type => :"db.type/string", Db.doc => "A person's email"} == entity_result3
     
    {:ok, entity_result4} = Db.entity([Db.ident, :"person/email"], [Db.cardinality])
    assert %{Db.cardinality => Db.cardinality_one} == entity_result4
  end

  test "Handles garbled queries" do
    query = [:find, Db.q?(:c), :"wh?ere"]
    {:error, query_result} = Db.q(query)
    assert Regex.match?(~r/Exception/, query_result)
  end
  
  test "Handles garbled transactions" do
    data_to_add = [%{ Db.id => :foobar }]
    {:error, transaction_result} = DatomicGenServer.transact(data_to_add)
    assert Regex.match?(~r/Exception/, transaction_result)
  end
  
  # TODO Add tests that use implicit/inS, bindings and find specifications,
  # and clauses.
  
end
