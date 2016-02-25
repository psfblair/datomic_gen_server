defmodule EntityMapTest do
  use ExUnit.Case, async: false
  alias DatomicGenServer.EntityMap, as: EntityMap
  
  defmodule TestPerson do
    defstruct id: nil, names: MapSet.new([]), age: nil
  end

  test "creates a new empty EntityMap" do
    new_map = EntityMap.new()
    assert new_map.inner_map == %{}
  end
  
  test "two empty EntityMaps are equal" do
    new_map = EntityMap.new()
    new_map2 = EntityMap.new()
    assert EntityMap.equal?(new_map, new_map2)
    assert new_map == new_map2
  end
  
  test "creates a new EntityMap with a list of datoms" do
    d1 = %Datom{e: 0, a: :attr1, v: :value, tx: 0, added: true}
    d2 = %Datom{e: 0, a: :attr2, v: :value2, tx: 0, added: true}
    d3 = %Datom{e: 1, a: :attr2, v: :value3, tx: 0, added: true}
    d4 = %Datom{e: 1, a: :attr3, v: :value2, tx: 0, added: false}
    
    expected_inner_map = %{
      0 => %{"datom/e": 0, attr1: :value, attr2: :value2},
      1 => %{"datom/e": 1, attr2: :value3}
    }
    expected_entity_map = %EntityMap{
      inner_map: expected_inner_map,
      index_by: nil,
      cardinality_many: [],
      aggregator: &EntityMap.default_aggregator/1
    }
    
    actual = EntityMap.new([d1, d2, d3, d4])
    
    assert actual.inner_map == expected_entity_map.inner_map
    assert actual.index_by == expected_entity_map.index_by
    assert EntityMap.equal?(actual, expected_entity_map)
  end
  
  test "two EntityMaps containing equal data are equal" do
    d1 = %Datom{e: 0, a: :attr1, v: :value, tx: 0, added: true}
    d2 = %Datom{e: 0, a: :attr2, v: :value2, tx: 0, added: true}
    d3 = %Datom{e: 1, a: :attr2, v: :value3, tx: 0, added: true}
    d4 = %Datom{e: 1, a: :attr3, v: :value2, tx: 0, added: false}
    
    new_map = EntityMap.new([d1, d2, d3, d4])
    new_map2 = EntityMap.new([d1, d2, d3, d4])
    assert EntityMap.equal?(new_map, new_map2)
    assert new_map == new_map2
  end
  
  test "creates a new EntityMap with a list of datoms, indexed by the specified attribute" do
    d1 = %Datom{e: 0, a: :attr1, v: :value1, tx: 0, added: true}
    d2 = %Datom{e: 0, a: :attr2, v: :value2, tx: 0, added: true}
    d3 = %Datom{e: 1, a: :attr1, v: :value3, tx: 0, added: true}
    d4 = %Datom{e: 1, a: :attr2, v: :value4, tx: 0, added: true}
        
    expected_inner_map = %{
      :value1 => %{"datom/e": 0, attr1: :value1, attr2: :value2},
      :value3 => %{"datom/e": 1, attr1: :value3, attr2: :value4},
    }
    
    result = EntityMap.new([d1, d2, d3, d4], index_by: :attr1)
    
    assert result.index_by == :attr1
    assert result.inner_map == expected_inner_map
  end
  
  test "creates a new EntityMap with a list of datoms, with a cardinality many attribute" do
    d1 = %Datom{e: 0, a: :attr1, v: :value1, tx: 0, added: true}
    d2 = %Datom{e: 0, a: :attr1, v: :value1a, tx: 0, added: true}
    d3 = %Datom{e: 0, a: :attr2, v: :value2, tx: 0, added: true}
    d4 = %Datom{e: 1, a: :attr1, v: :value3, tx: 0, added: true}
    d5 = %Datom{e: 1, a: :attr2, v: :value4, tx: 0, added: true}
        
    expected_inner_map = %{
      0 => %{"datom/e": 0, attr1: MapSet.new([:value1, :value1a]), attr2: :value2},
      1 => %{"datom/e": 1, attr1: MapSet.new([:value3]), attr2: :value4},
    }
    
    result = EntityMap.new([d1, d2, d3, d4, d5], cardinality_many: [:attr1])
    
    assert result.index_by == nil
    assert result.cardinality_many == MapSet.new([:attr1])
    assert result.inner_map == expected_inner_map
  end
  
  # This feature is more for records that may have collection values.
  test "merges values that are collections together if they are declared as cardinality many, but not otherwise" do
    d1 = %Datom{e: 0, a: :attr1, v: [:value1, :value1a], tx: 0, added: true}
    d2 = %Datom{e: 0, a: :attr1, v: MapSet.new([:value2]), tx: 0, added: true}
    d3 = %Datom{e: 1, a: :attr2, v: [:value3, :value3a], tx: 0, added: true}
    d4 = %Datom{e: 1, a: :attr2, v: [:value4], tx: 0, added: true}
        
    expected_inner_map = %{
      0 => %{"datom/e": 0, attr1: MapSet.new([:value1, :value1a, :value2])},
      1 => %{"datom/e": 1, attr2: [:value4]},
    }
    
    result = EntityMap.new([d1, d2, d3, d4], cardinality_many: [:attr1])
    
    assert result.index_by == nil
    assert result.cardinality_many == MapSet.new([:attr1])
    assert result.inner_map == expected_inner_map
  end
  
  test "creates a new, indexed EntityMap of structs from a list of datoms, with a cardinality many attribute" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 0, a: :identifier, v: :bill_smith, tx: 0, added: true}
    d4 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: false}
    d5 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    d6 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 0, added: true}
    d7 = %Datom{e: 2, a: :age, v: 23, tx: 0, added: false}
    d8 = %Datom{e: 2, a: :identifier, v: :jim_stewart, tx: 0, added: true}
    d9 = %Datom{e: 3, a: :name, v: "Hartley Stewart", tx: 0, added: false}
    d10 = %Datom{e: 3, a: :age, v: 44, tx: 0, added: false}
    d11 = %Datom{e: 3, a: :identifier, v: :hartley_stewart, tx: 0, added: false}
    
    # This is how you get your struct's default values in the aggregated result
    # if there is no value for that field in the incoming attribute map.
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{identifier: :id, name: :names})
        struct(TestPerson, struct_map)
      end

    expected_inner_map = %{
      :bill_smith => %TestPerson{id: :bill_smith, names: MapSet.new(["Bill Smith"]), age: 32},
      nil => %TestPerson{id: nil, names: MapSet.new([]), age: 64},
      :jim_stewart => %TestPerson{id: :jim_stewart, names: MapSet.new(["Jim Stewart"]), age: nil}
    }
    
    result = EntityMap.new([d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11], 
                cardinality_many: :name, index_by: :id, aggregator: aggregator)
    
    assert result.index_by == :id
    assert result.cardinality_many == MapSet.new([:name])
    assert result.inner_map == expected_inner_map
    assert result.aggregator == aggregator
  end
  
  test "merges record values that are collections together if they are declared as cardinality many, but not otherwise" do
    d1 = %{id: 1, attr1: [:value1, :value1a]}
    d2 = %{id: 1, attr1: MapSet.new([:value2])}
    d3 = %{id: 2, attr2: [:value3, :value3a]}
    d4 = %{id: 2, attr2: [:value4]}
        
    expected_inner_map = %{
      1 => %{id: 1, attr1: MapSet.new([:value1, :value1a, :value2])},
      2 => %{id: 2, attr2: [:value4]},
    }
    
    result = EntityMap.from_records([d1, d2, d3, d4], index_by: :id, cardinality_many: [:attr1])
    
    assert result.index_by == :id
    assert result.cardinality_many == MapSet.new([:attr1])
    assert result.inner_map == expected_inner_map
  end
    
  test "creates a new, indexed EntityMap of structs with a list of records, with a cardinality many attribute" do
    d1 = %{identifier: :bill_smith , name: "Bill Smith", age: 32}
    d2 = %{identifier: :bill_smith, name: "William Smith", age: 32}
    d3 = %{identifier: :karina_jones, name: "Karina Jones", age: 64}
    d4 = %{identifier: :jim_stewart, name: "Jim Stewart", age: 23}
    d5 = %{identifier: :hartley_stewart, name: "Hartley Stewart", age: 44}
    
    aggregator = 
      fn(attr_map) -> 
        %TestPerson{id: attr_map[:identifier], names: attr_map[:name], age: attr_map[:age]} 
      end
    
    expected_inner_map = %{
      :bill_smith => %TestPerson{id: :bill_smith, names: MapSet.new(["Bill Smith", "William Smith"]), age: 32},
      :karina_jones => %TestPerson{id: :karina_jones, names: MapSet.new(["Karina Jones"]), age: 64},
      :jim_stewart => %TestPerson{id: :jim_stewart, names: MapSet.new(["Jim Stewart"]), age: 23},
      :hartley_stewart => %TestPerson{id: :hartley_stewart, names: MapSet.new(["Hartley Stewart"]), age: 44}
    }

    result = EntityMap.from_records([d1, d2, d3, d4, d5], 
              cardinality_many: [:name], index_by: :identifier, aggregator: aggregator)
    
    assert result.index_by == :identifier
    assert result.cardinality_many == MapSet.new([:name])
    assert result.inner_map == expected_inner_map
  end
  
  # A 
  test "creates a new EntityMap with a set of rows and a header" do
    d1 = [:bill_smith, "Bill Smith", 32]
    d2 = [:bill_smith, "William Smith", 32]
    d3 = [:karina_jones, "Karina Jones", 64]
    d4 = [:jim_stewart, "Jim Stewart", 23]
    d5 = [:hartley_stewart, "Hartley Stewart", 44]
    
    header = [:identifier, :name, :age]
    records = MapSet.new([d1, d2, d3, d4, d5])
    
    expected_inner_map = %{
      :bill_smith => %{identifier: :bill_smith, name: MapSet.new(["Bill Smith", "William Smith"]), age: 32},
      :karina_jones => %{identifier: :karina_jones, name: MapSet.new(["Karina Jones"]), age: 64},
      :jim_stewart => %{identifier: :jim_stewart, name: MapSet.new(["Jim Stewart"]), age: 23},
      :hartley_stewart => %{identifier: :hartley_stewart, name: MapSet.new(["Hartley Stewart"]), age: 44}
    }

    result = EntityMap.from_rows(records, header, cardinality_many: [:name], index_by: :identifier)
    
    assert result.index_by == :identifier
    assert result.cardinality_many == MapSet.new([:name])
    assert result.inner_map == expected_inner_map
  end

  test "creates a new, indexed EntityMap of structs from a Datomic transaction, with a cardinality many attribute" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 0, a: :identifier, v: :bill_smith, tx: 0, added: true}
    d4 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
    d5 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d6 = %Datom{e: 1, a: :identifier, v: :karina_jones, tx: 0, added: false}
    d7 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: false}
    d8 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 0, added: true}
    d9 = %Datom{e: 2, a: :age, v: 23, tx: 0, added: false}
    d10 = %Datom{e: 2, a: :identifier, v: :jim_stewart, tx: 0, added: true}
    d11 = %Datom{e: 3, a: :name, v: "Hartley Stewart", tx: 0, added: false}
    d12 = %Datom{e: 3, a: :age, v: 44, tx: 0, added: false}
    d13 = %Datom{e: 3, a: :identifier, v: :hartley_stewart, tx: 0, added: false}
    
    added_datoms = [d1, d2, d3, d4, d5, d8, d10]
    retracted_datoms = [d6, d7, d9, d11, d12, d13]
     
    transaction = %DatomicTransaction{
      basis_t_before: 1000, 
      basis_t_after: 1001, 
      added_datoms: added_datoms, 
      retracted_datoms: retracted_datoms, 
      tempids: %{-1432323 => 64}
    }
  
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{identifier: :id, name: :names})
        struct(TestPerson, struct_map)
      end

    expected_inner_map = %{
      :bill_smith => %TestPerson{id: :bill_smith, names: MapSet.new(["Bill Smith", "William Smith"]), age: 32},
      nil => %TestPerson{id: nil, names: MapSet.new(["Karina Jones"]), age: nil},
      :jim_stewart => %TestPerson{id: :jim_stewart, names: MapSet.new(["Jim Stewart"]), age: nil}
    }
    
    result = EntityMap.from_transaction(transaction, 
              cardinality_many: [:name], index_by: :id, aggregator: aggregator)
    
    assert result.index_by == :id
    assert result.cardinality_many == MapSet.new([:name])
    assert result.inner_map == expected_inner_map
    assert result.aggregator == aggregator
  end
  
  # test "updates an empty EntityMap with datoms" do
  #   d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
  #   d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
  #   d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: false}
  #   d4 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
  #   d5 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 0, added: true}
  #   d6 = %Datom{e: 2, a: :age, v: 23, tx: 0, added: false}
  #   d7 = %Datom{e: 3, a: :name, v: "Hartley Stewart", tx: 0, added: false}
  #   d8 = %Datom{e: 3, a: :age, v: 44, tx: 0, added: false}
  #   
  #   added_datoms = [d1, d2, d4, d5]
  #   retracted_datoms = [d3, d6, d7, d8]
  #   
  #   empty_map = EntityMap.new()
  #   
  #   expected_inner_map = %{
  #     0 => %{"datom/e": 0, name: "Bill Smith", age: 32},
  #     1 => %{"datom/e": 1, age: 64},
  #     2 => %{"datom/e": 2, name: "Jim Stewart"}
  #   }
  #   
  #   result = EntityMap.update(empty_map, retracted_datoms, added_datoms)
  #   
  #   assert result.index_by == nil
  #   assert result.inner_map == expected_inner_map    
  # end
  # 
  # test "updates an EntityMap with datoms" do
  #   d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
  #   d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
  #   
  #   initial_map = EntityMap.new([d1, d2])
  #   
  #   d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: false}
  #   d4 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
  #   d5 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 0, added: true}
  #   d6 = %Datom{e: 2, a: :age, v: 23, tx: 0, added: false}
  #   d7 = %Datom{e: 3, a: :name, v: "Hartley Stewart", tx: 0, added: false}
  #   d8 = %Datom{e: 3, a: :age, v: 44, tx: 0, added: false}
  #   d9 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: false}
  #   d10 = %Datom{e: 0, a: :age, v: 63, tx: 0, added: true}
  #   
  #   added_datoms = [d4, d5, d10]
  #   retracted_datoms = [d3, d6, d7, d8, d9]
  #   
  #   expected_inner_map = %{
  #     0 => %{"datom/e": 0, name: "Bill Smith", age: 63},
  #     1 => %{"datom/e": 1, age: 64},
  #     2 => %{"datom/e": 2, name: "Jim Stewart"}
  #   }
  #   
  #   result = EntityMap.update(initial_map, retracted_datoms, added_datoms)
  #   
  #   assert result.index_by == nil
  #   assert result.inner_map == expected_inner_map        
  # end
  
  test "updating an EntityMap with datoms preserves indexing and aggregation" do
    
  end
  
  test "updates an EntityMap with records" do
    
  end
  
  test "updating an EntityMap with records preserves indexing and aggregation" do
    
  end
  
  test "updates an EntityMap with a transaction" do
    
  end
  
  test "updating an EntityMap with a transaction preserves indexing and aggregation" do
    
  end
  
  test "updating with a second added value for an attribute makes the attribute value a collection" do
  end
  
  test "retracting a value for an attribute whose value is a collection removes the value from the collection" do
    
  end
  
  test "any value that a record contains for an attribute replaces the value in the entity map" do
  end
  

end
