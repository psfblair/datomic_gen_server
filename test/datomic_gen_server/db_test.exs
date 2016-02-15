defmodule DatomicGenServer.DbTest do
  use ExUnit.Case, async: false
  alias DatomicGenServer.Db, as: Db
  
  setup_all do
    # Need long timeouts to let the JVM start.
    DatomicGenServer.start_link(
      "datomic:mem://test", 
      true, 
      [{:timeout, 20_000}, {:default_message_timeout, 20_000}, {:name, DatomicGenServer}]
    )
    :ok
  end
  
  test "can issue Datomic queries" do
    query = [:find, Db.q?(:c), :where, [Db.q?(:c), Db.doc, "Some docstring that shouldn't be in the database"]]
    result = Db.q(DatomicGenServer, query)
    
    empty_set = MapSet.new()
    assert {:ok, empty_set} == result

    second_result = Db.q(DatomicGenServer, query)
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
    
    {:ok, transaction_result} = Db.transact(DatomicGenServer, data_to_add)
    assert is_integer(transaction_result.basis_t_before)
    assert is_integer(transaction_result.basis_t_after)
    assert (transaction_result.basis_t_after - transaction_result.basis_t_before) > 0
    
    retracted_datoms = transaction_result.retracted_datoms
    assert 0 == Enum.count(retracted_datoms)
    
    added_datoms = transaction_result.added_datoms
    assert 6 == Enum.count(added_datoms)
    
    first_datom = hd(added_datoms)
    assert is_integer(first_datom.e)
    assert is_integer(first_datom.a)
    assert ! is_nil(first_datom.v)
    assert is_integer(first_datom.tx)
    assert first_datom.added
    
    tempids = transaction_result.tempids
    assert 1 == Enum.count(tempids)
    assert (Map.keys(tempids) |> hd |> is_integer)
    assert (Map.values(tempids) |> hd |> is_integer)

    query = [:find, Db.q?(:c), :where, [Db.q?(:c), Db.doc, "A person's name"]]
    {:ok, query_result} = Db.q(DatomicGenServer, query)
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
    {:ok, _} = Db.transact(DatomicGenServer, data_to_add)

    all_attributes = 
      %{ Db.ident => :"person/email", 
         Db.value_type => Db.type_string, 
         Db.cardinality => Db.cardinality_one, 
         Db.doc => "A person's email"
        }
      
    {:ok, entity_result} = Db.entity(DatomicGenServer, :"person/email")
    assert all_attributes == entity_result

    {:ok, entity_result2} = Db.entity(DatomicGenServer, :"person/email", :all)
    assert all_attributes == entity_result2

    {:ok, entity_result3} = Db.entity(DatomicGenServer, :"person/email", [Db.value_type, Db.doc])
    assert %{Db.value_type => Db.type_string, Db.doc => "A person's email"} == entity_result3
     
    {:ok, entity_result4} = Db.entity(DatomicGenServer, [Db.ident, :"person/email"], [Db.cardinality])
    assert %{Db.cardinality => Db.cardinality_one} == entity_result4
  end
  
  defmodule TestQueryResponse do
    defstruct id: nil, identity: nil
  end

  test "Can convert a query response to a list of structs" do
    seed_data = [%{ 
        Db.id => Db.dbid(Db.schema_partition),
        Db.ident => :"business/name",
        Db.value_type => Db.type_string,
        Db.cardinality => Db.cardinality_one,
        Db.doc => "A business's name",
        Db.install_attribute => Db.schema_partition
    }]
    {:ok, _} = Db.transact(DatomicGenServer, seed_data)
    
    converter = fn(exdn) -> 
      case exdn do
        [id, ident] -> %TestQueryResponse{id: id, identity: ident}
        _ -> exdn
      end
    end
    
    query = [:find, Db.q?(:e), Db.q?(:ident), 
             :where, [Db.q?(:e), :"db/doc", "A business's name"],
                     [Db.q?(:e), Db.ident, Db.q?(:ident)]]
    {:ok, query_result} = Db.q(DatomicGenServer, query, [{:response_converter, converter}])
    [%TestQueryResponse{id: entity_id, identity: :"business/name"}] = MapSet.to_list(query_result)
    assert is_integer(entity_id)
  end
  
  defmodule TestEntityResponse do
    defstruct "db/ident": nil, "db/valueType": nil, "db/cardinality": nil, "db/doc": nil
  end
  
  test "Can convert an entity response to a struct" do
    seed_data = [%{ 
        Db.id => Db.dbid(Db.schema_partition),
        Db.ident => :"business/email",
        Db.value_type => Db.type_string,
        Db.cardinality => Db.cardinality_one,
        Db.doc => "A business's email",
        Db.install_attribute => Db.schema_partition
    }]
    
    {:ok, _} = Db.transact(DatomicGenServer, seed_data)
    
    converter = fn(exdn) -> 
      case exdn do
        %{"db/ident": _} -> struct(TestEntityResponse, exdn) 
        _ -> exdn
      end
    end

    {:ok, entity_result} = Db.entity(DatomicGenServer, :"business/email", :all, [{:response_converter, converter}])
    
    %TestEntityResponse{
        "db/ident": :"business/email", 
        "db/valueType": :"db.type/string", 
        "db/cardinality": :"db.cardinality/one", 
        "db/doc": "A business's email" } = entity_result
  end

  test "Handles garbled queries" do
    query = [:find, Db.q?(:c), :"wh?ere"]
    {:error, query_result} = Db.q(DatomicGenServer, query)
    assert Regex.match?(~r/Exception/, query_result)
  end
  
  test "Handles garbled transactions" do
    data_to_add = [%{ Db.id => :foobar, some: :other }]
    {:error, transaction_result} = Db.transact(DatomicGenServer, data_to_add)
    assert Regex.match?(~r/Exception/, transaction_result)
  end
  
  # TODO Add tests that use implicit/inS, bindings and find specifications,
  # and clauses.
  
end
