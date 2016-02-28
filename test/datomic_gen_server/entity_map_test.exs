defmodule EntityMapTest do
  use ExUnit.Case, async: false
  alias DatomicGenServer.EntityMap, as: EntityMap
  alias DatomicGenServer.EntityMap.DataTuple, as: DataTuple
  
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
  
  test "creates a new EntityMap with a list of datoms, which may contain entities of different types" do
    d1 = %Datom{e: 0, a: :attr1, v: :value, tx: 0, added: true}
    d2 = %Datom{e: 0, a: :attr2, v: :value2, tx: 0, added: true}
    d3 = %Datom{e: 1, a: :attr2, v: :value3, tx: 0, added: true}
    d4 = %Datom{e: 1, a: :attr3, v: :value2, tx: 0, added: false}
    d5 = %Datom{e: 2, a: :attr4, v: :value3, tx: 0, added: true}
    d6 = %Datom{e: 2, a: :attr5, v: :value5, tx: 0, added: true}
    
    expected_inner_map = %{
      0 => %{"datom/e": 0, attr1: :value, attr2: :value2},
      1 => %{"datom/e": 1, attr2: :value3},
      2 => %{"datom/e": 2, attr4: :value3, attr5: :value5}
    }
    
    expected_entity_map = %EntityMap{
      inner_map: expected_inner_map,
      index_by: nil,
      cardinality_many: [],
      aggregator: &EntityMap.default_aggregator/1
    }
    
    actual = EntityMap.new([d1, d2, d3, d4, d5, d6])
    
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
    
    result = EntityMap.new([d1, d2, d3, d4], index_by: :attr1)
        
    expected_inner_map = %{
      :value1 => %{"datom/e": 0, attr1: :value1, attr2: :value2},
      :value3 => %{"datom/e": 1, attr1: :value3, attr2: :value4},
    }
    
    assert result.index_by == :attr1
    assert result.inner_map == expected_inner_map
  end
  
  test "creates a new EntityMap with a list of datoms, with a cardinality many attribute" do
    d1 = %Datom{e: 0, a: :attr1, v: :value1, tx: 0, added: true}
    d2 = %Datom{e: 0, a: :attr1, v: :value1a, tx: 0, added: true}
    d3 = %Datom{e: 0, a: :attr2, v: :value2, tx: 0, added: true}
    d4 = %Datom{e: 1, a: :attr1, v: :value3, tx: 0, added: true}
    d5 = %Datom{e: 1, a: :attr2, v: :value4, tx: 0, added: true}
    
    result = EntityMap.new([d1, d2, d3, d4, d5], cardinality_many: [:attr1])
        
    expected_inner_map = %{
      0 => %{"datom/e": 0, attr1: MapSet.new([:value1, :value1a]), attr2: :value2},
      1 => %{"datom/e": 1, attr1: MapSet.new([:value3]), attr2: :value4},
    }
    
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
      
    result = EntityMap.new([d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11], 
                cardinality_many: :name, index_by: :id, aggregator: aggregator)
  
    expected_inner_map = %{
      :bill_smith => %TestPerson{id: :bill_smith, names: MapSet.new(["Bill Smith"]), age: 32},
      :jim_stewart => %TestPerson{id: :jim_stewart, names: MapSet.new(["Jim Stewart"]), age: nil}
    }
    
    assert result.index_by == :id
    assert result.cardinality_many == MapSet.new([:name])
    assert result.inner_map == expected_inner_map
    assert result.aggregator == aggregator
  end
  
  test "a nil value or empty collection value in a data tuple nullifies the given attribute
        and entities with no attributes are removed." do
    d1 = %DataTuple{e: 0, a: :attr1, v: :value1, added: true}
    d2 = %DataTuple{e: 0, a: :attr1, v: :value1a, added: true}
    d3 = %DataTuple{e: 0, a: :attr1, v: nil, added: true}    
    d4 = %DataTuple{e: 0, a: :attr2, v: :value2, added: true}
    d5 = %DataTuple{e: 0, a: :attr3, v: :value3, added: true}
    d6 = %DataTuple{e: 0, a: :attr3, v: nil, added: true}
    d7 = %DataTuple{e: 1, a: :attr1, v: :value1b, added: true}
    d8 = %DataTuple{e: 1, a: :attr1, v: [], added: true}
    d9 = %DataTuple{e: 1, a: :attr2, v: :value2a, added: true}
    d10 = %DataTuple{e: 2, a: :attr1, v: :value1b, added: true}
    d11 = %DataTuple{e: 2, a: :attr1, v: MapSet.new(), added: true}
    d12 = %DataTuple{e: 2, a: :attr2, v: :value2b, added: true}
    d13 = %DataTuple{e: 3, a: :attr1, v: nil, added: true}
    d14 = %DataTuple{e: 3, a: :attr2, v: :value2c, added: true}
    d15 = %DataTuple{e: 4, a: :attr1, v: [], added: true}
    d16 = %DataTuple{e: 4, a: :attr2, v: :value2d, added: true}
    d17 = %DataTuple{e: 5, a: :attr1, v: MapSet.new(), added: true}
    d18 = %DataTuple{e: 5, a: :attr2, v: nil, added: true}
    
    result = EntityMap.new([d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12,
                            d13, d14, d15, d16, d17, d18], cardinality_many: [:attr1])
        
    expected_inner_map = %{
      0 => %{"datom/e": 0, attr2: :value2},
      1 => %{"datom/e": 1, attr2: :value2a},
      2 => %{"datom/e": 2, attr2: :value2b},
      3 => %{"datom/e": 3, attr2: :value2c},
      4 => %{"datom/e": 4, attr2: :value2d},
    }
    
    assert result.index_by == nil
    assert result.cardinality_many == MapSet.new([:attr1])
    assert result.inner_map == expected_inner_map
  end
    
  test "creates a new, indexed EntityMap of structs with a list of records, with a cardinality many attribute" do
    d1 = %{eid: 1, unique_name: :bill_smith, name: "Bill Smith", age: 32}
    # If we have 2 records for an entity, cardinality many attributes are aggregated;
    # for cardinality one attributes the last one wins.
    d2 = %{eid: 1, unique_name: :bill_smith, name: "William Smith", age: 32}
    d3 = %{eid: 2, unique_name: :karina_jones, name: "Karina Jones", age: 64}
    d4 = %{eid: 3, unique_name: :jim_stewart, name: "Jim Stewart", age: 23}
    d5 = %{eid: 4, unique_name: :hartley_stewart, name: "Hartley Stewart", age: 44}
    
    aggregator = 
      fn(attr_map) -> 
        %TestPerson{id: attr_map[:unique_name], names: attr_map[:name], age: attr_map[:age]} 
      end
      
    result = EntityMap.from_records([d1, d2, d3, d4, d5], :eid, 
              cardinality_many: [:name], index_by: :id, aggregator: aggregator)
    
    expected_inner_map = %{
      :bill_smith => %TestPerson{id: :bill_smith, names: MapSet.new(["Bill Smith", "William Smith"]), age: 32},
      :karina_jones => %TestPerson{id: :karina_jones, names: MapSet.new(["Karina Jones"]), age: 64},
      :jim_stewart => %TestPerson{id: :jim_stewart, names: MapSet.new(["Jim Stewart"]), age: 23},
      :hartley_stewart => %TestPerson{id: :hartley_stewart, names: MapSet.new(["Hartley Stewart"]), age: 44}
    }
    
    assert result.index_by == :id
    assert result.cardinality_many == MapSet.new([:name])
    assert result.inner_map == expected_inner_map
  end
  
  test "creates an EntityMap of structs with a list of records containing collection values,
        some of which are empty, for a cardinality many attribute" do
    d1 = %{eid: 1, unique_name: :bill_smith, name: ["Bill Smith", "William Smith"], age: 32}
    d2 = %{eid: 2, unique_name: :karina_jones, name: ["Karina Jones"], age: 64}
    d3 = %{eid: 2, unique_name: :karina_jones, name: nil, age: 64}
    d4 = %{eid: 3, unique_name: :jim_stewart, name: ["Jim Stewart"], age: 23}
    d5 = %{eid: 3, unique_name: :jim_stewart, name: [], age: 23}
    d6 = %{eid: 4, unique_name: :hartley_stewart, name: ["Hartley Stewart"], age: 44}
    d7 = %{eid: 4, unique_name: :hartley_stewart, name: MapSet.new(), age: 44}
    
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{unique_name: :id, name: :names})
        struct(TestPerson, struct_map)
      end
      
    result = EntityMap.from_records([d1, d2, d3, d4, d5, d6, d7], :eid, 
              cardinality_many: [:name], index_by: :id, aggregator: aggregator)
    
    expected_inner_map = %{
      :bill_smith => %TestPerson{id: :bill_smith, names: MapSet.new(["Bill Smith", "William Smith"]), age: 32},
      :karina_jones => %TestPerson{id: :karina_jones, names: MapSet.new(), age: 64},
      :jim_stewart => %TestPerson{id: :jim_stewart, names: MapSet.new(), age: 23},
      :hartley_stewart => %TestPerson{id: :hartley_stewart, names: MapSet.new(), age: 44}
    }
    
    assert result.index_by == :id
    assert result.cardinality_many == MapSet.new([:name])
    assert result.inner_map == expected_inner_map
  end
  
  test "creates a new EntityMap with a set of rows and a header" do
    d1 = [1, :bill_smith, "Bill Smith", 32]
    d2 = [1, :bill_smith, "William Smith", 32]
    d3 = [2, :karina_jones, "Karina Jones", 64]
    d4 = [3, :jim_stewart, "Jim Stewart", 23]
    d5 = [4, :hartley_stewart, "Hartley Stewart", 44]
    
    header = [:eid, :unique_name, :name, :age]
    records = MapSet.new([d1, d2, d3, d4, d5])
  
    result = EntityMap.from_rows(records, header, :eid, cardinality_many: [:name], index_by: :unique_name)
    
    expected_inner_map = %{
      :bill_smith => %{"datom/e": 1, eid: 1, unique_name: :bill_smith, name: MapSet.new(["Bill Smith", "William Smith"]), age: 32},
      :karina_jones => %{"datom/e": 2, eid: 2, unique_name: :karina_jones, name: MapSet.new(["Karina Jones"]), age: 64},
      :jim_stewart => %{"datom/e": 3, eid: 3, unique_name: :jim_stewart, name: MapSet.new(["Jim Stewart"]), age: 23},
      :hartley_stewart => %{"datom/e": 4, eid: 4, unique_name: :hartley_stewart, name: MapSet.new(["Hartley Stewart"]), age: 44}
    }
    
    assert result.inner_map == expected_inner_map
    assert result.index_by == :unique_name
    assert result.cardinality_many == MapSet.new([:name])
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
      
    result = EntityMap.from_transaction(transaction, 
              cardinality_many: [:name], index_by: :id, aggregator: aggregator)
  
    expected_inner_map = %{
      :bill_smith => %TestPerson{id: :bill_smith, names: MapSet.new(["Bill Smith", "William Smith"]), age: 32},
      :jim_stewart => %TestPerson{id: :jim_stewart, names: MapSet.new(["Jim Stewart"]), age: nil}
    }
    
    assert result.index_by == :id
    assert result.cardinality_many == MapSet.new([:name])
    assert result.inner_map == expected_inner_map
    assert result.aggregator == aggregator
  end
  
  test "updates an empty EntityMap with datoms" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: false}
    d4 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    d5 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 0, added: true}
    d6 = %Datom{e: 2, a: :age, v: 23, tx: 0, added: false}
    d7 = %Datom{e: 3, a: :name, v: "Hartley Stewart", tx: 0, added: false}
    d8 = %Datom{e: 3, a: :age, v: 44, tx: 0, added: false}
    
    datoms_to_update = [d1, d2, d3, d4, d5, d6, d7, d8]
    
    empty_map = EntityMap.new()
    
    result = EntityMap.update(empty_map, datoms_to_update)
    
    expected_inner_map = %{
      0 => %{"datom/e": 0, name: "Bill Smith", age: 32},
      1 => %{"datom/e": 1, age: 64},
      2 => %{"datom/e": 2, name: "Jim Stewart"}
    }
    
    assert result.index_by == nil
    assert result.inner_map == expected_inner_map    
  end
  
  test "updates an EntityMap with datoms" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    
    initial_map = EntityMap.new([d1, d2])
    
    d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: false}
    d4 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    d5 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 0, added: true}
    d6 = %Datom{e: 2, a: :age, v: 23, tx: 0, added: false}
    d7 = %Datom{e: 3, a: :name, v: "Hartley Stewart", tx: 0, added: false}
    d8 = %Datom{e: 3, a: :age, v: 44, tx: 0, added: false}
    d9 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: false}
    d10 = %Datom{e: 0, a: :age, v: 63, tx: 0, added: true}
    
    datoms_to_update = [d3, d4, d5, d6, d7, d8, d9, d10]
    
    result = EntityMap.update(initial_map, datoms_to_update)
    
    expected_inner_map = %{
      0 => %{"datom/e": 0, name: "Bill Smith", age: 63},
      1 => %{"datom/e": 1, age: 64},
      2 => %{"datom/e": 2, name: "Jim Stewart"}
    }
    
    assert result.index_by == nil
    assert result.inner_map == expected_inner_map        
  end
  
  test "updating an EntityMap with datoms preserves indexing and aggregation" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 0, a: :identifier, v: :bill_smith, tx: 0, added: true}
    d4 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d5 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    d6 = %Datom{e: 1, a: :identifier, v: :karina_jones, tx: 0, added: true}
    
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{identifier: :id, name: :names})
        struct(TestPerson, struct_map)
      end
  
    initial_map = EntityMap.new([d1, d2, d3, d4, d5, d6], 
                    cardinality_many: :name, index_by: :id, aggregator: aggregator)
  
    d7 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 1, added: true}
    d8 = %Datom{e: 2, a: :age, v: 23, tx: 1, added: true}
    d9 = %Datom{e: 2, a: :identifier, v: :jim_stewart, tx: 1, added: true}
    d10 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 1, added: false}
    d11 = %Datom{e: 1, a: :name, v: "Karen Jones", tx: 1, added: true}
    d12 = %Datom{e: 0, a: :age, v: 29, tx: 1, added: true}
    
    datoms_to_update = [d7, d8, d9, d10, d11, d12]
    
    result = EntityMap.update(initial_map, datoms_to_update)
  
    expected_inner_map = %{
      :bill_smith => %TestPerson{id: :bill_smith, names: MapSet.new(["Bill Smith"]), age: 29},
      :karina_jones => %TestPerson{id: :karina_jones, names: MapSet.new(["Karen Jones"]), age: 64},
      :jim_stewart => %TestPerson{id: :jim_stewart, names: MapSet.new(["Jim Stewart"]), age: 23}
    }
    
    assert result.inner_map == expected_inner_map
    assert result.index_by == :id
    assert result.cardinality_many == MapSet.new([:name])
    assert result.aggregator == aggregator    
  end
  
  test "In retracting a value in an EntityMap containing attribute maps, an 
        attribute key is removed when the value is equal to the old value" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    
    initial_map = EntityMap.new([d1, d2, d3, d4])
  
    d5 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: false}
    d6 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: false}
    d7 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: false}
    d8 = %Datom{e: 1, a: :age, v: 10, tx: 0, added: false}
    
    result = EntityMap.update(initial_map, [d5, d6, d7, d8])
    
    expected_inner_map = %{
      1 => %{"datom/e": 1, age: 64},
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In retracting a value in an EntityMap containing attribute maps, an 
        attribute key is removed when the passed-in value is a set or a list and 
        contains the same elements as the old value" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d5 = %Datom{e: 1, a: :name, v: "Karen Jones", tx: 0, added: true}
    d6 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    
    initial_map = EntityMap.new([d1, d2, d3, d4, d5, d6], cardinality_many: :name)
  
    d7 = %Datom{e: 0, a: :name, v: ["Bill Smith", "William Smith"], tx: 0, added: false}
    d8 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: false}
    d9 = %Datom{e: 1, a: :name, v: MapSet.new(["Karina Jones"]), tx: 0, added: false}
    
    result = EntityMap.update(initial_map, [d7, d8, d9])
    
    expected_inner_map = %{
      1 => %{"datom/e": 1, name: MapSet.new(["Karen Jones"]), age: 64},
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In retracting a value in an EntityMap containing attribute maps, an 
        attribute key is removed when the value passed in is nil or empty" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    
    initial_map = EntityMap.new([d1, d2, d3, d4])
  
    d5 = %Datom{e: 0, a: :name, v: nil, tx: 0, added: false}
    d6 = %Datom{e: 0, a: :age, v: [], tx: 0, added: false}
    d7 = %Datom{e: 1, a: :name, v: MapSet.new([]), tx: 0, added: false}
    
    result = EntityMap.update(initial_map, [d5, d6, d7])
    
    expected_inner_map = %{
      1 => %{"datom/e": 1, age: 64},
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In retracting a value in an EntityMap containing attribute maps, an attribute 
        key is removed when the value is nil or empty and the old value is a set" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
    d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :name, v: "Karen Jones", tx: 0, added: true}
    d5 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    d6 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 0, added: true}
    
    initial_map = EntityMap.new([d1, d2, d3, d4, d5, d6], cardinality_many: :name)
  
    d7 = %Datom{e: 0, a: :name, v: nil, tx: 0, added: false}
    d8 = %Datom{e: 1, a: :name, v: [], tx: 0, added: false}
    d9 = %Datom{e: 2, a: :name, v: MapSet.new([]), tx: 0, added: false}
    
    result = EntityMap.update(initial_map, [d7, d8, d9])
    
    expected_inner_map = %{
      1 => %{"datom/e": 1, age: 64},
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In retracting a value in an EntityMap containing attribute maps, when the 
        old value is a set and the update value is a set or list, the new attribute 
        value is the old value less the elements of the update value" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
    d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :name, v: "Karen Jones", tx: 0, added: true}
    
    initial_map = EntityMap.new([d1, d2, d3, d4], cardinality_many: :name)
  
    d5 = %Datom{e: 0, a: :name, v: ["William Smith"], tx: 0, added: false}
    d6 = %Datom{e: 1, a: :name, v: MapSet.new(["Karina Jones"]), tx: 0, added: false}
    
    result = EntityMap.update(initial_map, [d5, d6])
    
    expected_inner_map = %{
      0 => %{"datom/e": 0, name: MapSet.new(["Bill Smith"])},
      1 => %{"datom/e": 1, name: MapSet.new(["Karen Jones"])},
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In retracting a value in an EntityMap containing attribute maps, when the 
        old value is a set and the update value is not a collection, the new 
        attribute value is the old value less the update value" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
    d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :name, v: "Karen Jones", tx: 0, added: true}
  
    initial_map = EntityMap.new([d1, d2, d3, d4], cardinality_many: :name)
  
    d5 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: false}
    d6 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: false}
    
    result = EntityMap.update(initial_map, [d5, d6])
    
    expected_inner_map = %{
      0 => %{"datom/e": 0, name: MapSet.new(["Bill Smith"])},
      1 => %{"datom/e": 1, name: MapSet.new(["Karen Jones"])},
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In retracting a value in an EntityMap containing structs, an attribute is 
        set to its default value when the value is equal to the old value" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{"datom/e": :id, name: :names})
        struct(TestPerson, struct_map)
      end
    
    initial_map = EntityMap.new([d1, d2, d3, d4], aggregator: aggregator)
  
    d5 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: false}
    d6 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: false}
    d7 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: false}
    d8 = %Datom{e: 1, a: :age, v: 10, tx: 0, added: false}
    
    result = EntityMap.update(initial_map, [d5, d6, d7, d8])
    
    expected_inner_map = %{
      1 => %TestPerson{id: 1, names: MapSet.new(), age: 64},
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In retracting a value in an EntityMap containing structs, an attribute is 
        set to its default value when the passed-in value is a set or a list and 
        contains the same elements as the old value" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d5 = %Datom{e: 1, a: :name, v: "Karen Jones", tx: 0, added: true}
    d6 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
  
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{"datom/e": :id, name: :names})
        struct(TestPerson, struct_map)
      end
    
    initial_map = EntityMap.new([d1, d2, d3, d4, d5, d6], 
                    cardinality_many: :name, aggregator: aggregator)
  
    d7 = %Datom{e: 0, a: :name, v: ["Bill Smith", "William Smith"], tx: 0, added: false}
    d8 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: false}
    d9 = %Datom{e: 1, a: :name, v: MapSet.new(["Karina Jones"]), tx: 0, added: false}
    
    result = EntityMap.update(initial_map, [d7, d8, d9])
    
    expected_inner_map = %{
      1 => %TestPerson{id: 1, names: MapSet.new(["Karen Jones"]), age: 64},
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In retracting a value in an EntityMap containing structs, an attribute is 
        set to its default value when the value passed in is nil or empty" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    d5 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 0, added: true}
    d6 = %Datom{e: 2, a: :age, v: 45, tx: 0, added: true}
    
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{"datom/e": :id, name: :names})
        struct(TestPerson, struct_map)
      end
  
    initial_map = EntityMap.new([d1, d2, d3, d4, d5, d6], aggregator: aggregator)
  
    d7 = %Datom{e: 0, a: :name, v: nil, tx: 0, added: false}
    d8 = %Datom{e: 1, a: :name, v: MapSet.new([]), tx: 0, added: false}
    d9 = %Datom{e: 2, a: :age, v: [], tx: 0, added: false}
    
    result = EntityMap.update(initial_map, [d7, d8, d9])
    
    expected_inner_map = %{
      0 => %TestPerson{id: 0, names: MapSet.new(), age: 32},
      1 => %TestPerson{id: 1, names: MapSet.new(), age: 64},
      2 => %TestPerson{id: 2, names: "Jim Stewart", age: nil},
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In retracting a value in an EntityMap containing structs, an attribute is set to 
        its default value when the value is nil or empty and the old value is a set" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
    d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :name, v: "Karen Jones", tx: 0, added: true}
    d5 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    d6 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 0, added: true}
    
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{"datom/e": :id, name: :names})
        struct(TestPerson, struct_map)
      end
    
    initial_map = EntityMap.new([d1, d2, d3, d4, d5, d6], 
                    cardinality_many: :name, aggregator: aggregator)
  
    d7 = %Datom{e: 0, a: :name, v: nil, tx: 0, added: false}
    d8 = %Datom{e: 1, a: :name, v: [], tx: 0, added: false}
    d9 = %Datom{e: 2, a: :name, v: MapSet.new([]), tx: 0, added: false}
    
    result = EntityMap.update(initial_map, [d7, d8, d9])
    
    expected_inner_map = %{
      #Empty entities are removed
      1 => %TestPerson{id: 1, names: MapSet.new(), age: 64},
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In retracting a value in an EntityMap containing structs, when the old value
        is a set and the update value is a set or list, the new attribute value is
        the old value less the elements of the update value" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
    d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :name, v: "Karen Jones", tx: 0, added: true}
    
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{"datom/e": :id, name: :names})
        struct(TestPerson, struct_map)
      end
    
    initial_map = EntityMap.new([d1, d2, d3, d4], 
                    cardinality_many: :name, aggregator: aggregator)
  
    d5 = %Datom{e: 0, a: :name, v: ["William Smith"], tx: 0, added: false}
    d6 = %Datom{e: 1, a: :name, v: MapSet.new(["Karina Jones"]), tx: 0, added: false}
    
    result = EntityMap.update(initial_map, [d5, d6])
    
    expected_inner_map = %{
      0 => %TestPerson{id: 0, names: MapSet.new(["Bill Smith"]), age: nil},
      1 => %TestPerson{id: 1, names: MapSet.new(["Karen Jones"]), age: nil},
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In retracting a value in an EntityMap containing structs, when the old value
        is a set and the update value is not a collection, the new attribute value
        is the old value less the update value" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
    d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :name, v: "Karen Jones", tx: 0, added: true}
    
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{"datom/e": :id, name: :names})
        struct(TestPerson, struct_map)
      end
  
    initial_map = EntityMap.new([d1, d2, d3, d4], 
                    cardinality_many: :name, aggregator: aggregator)
  
    d5 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: false}
    d6 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: false}
    
    result = EntityMap.update(initial_map, [d5, d6])
    
    expected_inner_map = %{
      0 => %TestPerson{id: 0, names: MapSet.new(["Bill Smith"]), age: nil},
      1 => %TestPerson{id: 1, names: MapSet.new(["Karen Jones"]), age: nil},
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In adding a value to an EntityMap containing attribute maps, when the attribute
        is not cardinality many, a new scalar value overwrites an old one" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    d5 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 0, added: true}
    d6 = %Datom{e: 3, a: :name, v: "Hartley Stewart", tx: 0, added: true}
    
    initial_map = EntityMap.new([d1, d2, d3, d4, d5, d6])
  
    d7 = %Datom{e: 0, a: :name, v: "William Smith", tx: 1, added: true}
    d8 = %Datom{e: 0, a: :newattr, v: "new", tx: 1, added: true}
    d9 = %Datom{e: 1, a: :name, v: nil, tx: 1, added: true}
    d10 = %Datom{e: 2, a: :name, v: [], tx: 1, added: true}
    d11 = %Datom{e: 3, a: :name, v: MapSet.new(), tx: 1, added: true}
    
    result = EntityMap.update(initial_map, [d7, d8, d9, d10, d11])
    
    expected_inner_map = %{
      0 => %{"datom/e": 0, name: "William Smith", age: 32, newattr: "new"},
      1 => %{"datom/e": 1, age: 64},
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In adding a value to an EntityMap containing attribute maps, when the attribute
        is cardinality many, a new scalar value is added to the existing collection." do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
  
    initial_map = EntityMap.new([d1, d2, d3], cardinality_many: :name)
  
    d4 = %Datom{e: 0, a: :name, v: "Billy Smith", tx: 1, added: true}
    
    result = EntityMap.update(initial_map, [d4])
    
    expected_inner_map = %{
      0 => %{"datom/e": 0, name: MapSet.new(["William Smith", "Bill Smith", "Billy Smith"]), age: 32}
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In adding a value to an EntityMap containing attribute maps, when the attribute
        is cardinality many, a nil or empty collection nullifies the existing collection." do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d5 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    d6 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 0, added: true}
  
    initial_map = EntityMap.new([d1, d2, d3, d4, d5, d6], cardinality_many: :name)
  
    d7 = %Datom{e: 0, a: :name, v: nil, tx: 1, added: true}
    d8 = %Datom{e: 1, a: :name, v: "Karen Jones", tx: 1, added: true}
    d9 = %DataTuple{e: 1, a: :name, v: [], added: true}
    d10 = %DataTuple{e: 2, a: :name, v: MapSet.new(), added: true}
    
    result = EntityMap.update(initial_map, [d7, d8, d9, d10])
    
    expected_inner_map = %{
      0 => %{"datom/e": 0, age: 32},
      1 => %{"datom/e": 1, age: 64}
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In adding a value to an EntityMap containing attribute maps, when the attribute 
        is cardinality many, a new collection value is added to the existing collection." do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    
    initial_map = EntityMap.new([d1, d2, d3, d4], cardinality_many: :name)
  
    d5 = %Datom{e: 0, a: :name, v: ["Billy Smith", "B. Smith"], tx: 1, added: true}
    d6 = %Datom{e: 1, a: :name, v: MapSet.new(["Karen Jones"]), tx: 1, added: true}
    
    result = EntityMap.update(initial_map, [d5, d6])
    
    expected_inner_map = %{
      0 => %{"datom/e": 0, name: MapSet.new(["Bill Smith", "William Smith", "Billy Smith", "B. Smith"]), age: 32},
      1 => %{"datom/e": 1, name: MapSet.new(["Karina Jones", "Karen Jones"])},
    }
    
    assert result.inner_map == expected_inner_map 
  end
    
  test "In adding a value to an EntityMap containing structs, when the attribute
        is not cardinality many, a new scalar value overwrites an old one" do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    d5 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 0, added: true}
    d6 = %Datom{e: 3, a: :name, v: "Hartley Stewart", tx: 0, added: true}
    
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{"datom/e": :id, name: :names})
        struct(TestPerson, struct_map)
      end
    
    initial_map = EntityMap.new([d1, d2, d3, d4, d5, d6], aggregator: aggregator)
  
    d7 = %Datom{e: 0, a: :name, v: "William Smith", tx: 1, added: true}
    d8 = %Datom{e: 1, a: :name, v: nil, tx: 1, added: true}
    d9 = %Datom{e: 2, a: :name, v: [], tx: 1, added: true}
    d10 = %Datom{e: 3, a: :name, v: MapSet.new(), tx: 1, added: true}
    
    result = EntityMap.update(initial_map, [d7, d8, d9, d10])
    
    expected_inner_map = %{
      0 => %TestPerson{id: 0, names: "William Smith", age: 32},
      1 => %TestPerson{id: 1, names: MapSet.new(), age: 64},
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In adding a value to an EntityMap containing structs, when the attribute is 
        cardinality many, a new scalar value is added to the existing collection." do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
  
    initial_map = EntityMap.new([d1, d2, d3], cardinality_many: :name)
  
    d4 = %Datom{e: 0, a: :name, v: "Billy Smith", tx: 1, added: true}
    
    result = EntityMap.update(initial_map, [d4])
    
    expected_inner_map = %{
      0 => %{"datom/e": 0, name: MapSet.new(["William Smith", "Bill Smith", "Billy Smith"]), age: 32}
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In adding a value to an EntityMap containing structs, when the attribute is 
        cardinality many, a nil resets the attribute to its default value." do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    d5 = %Datom{e: 1, a: :age, v: 64, tx: 0, added: true}
    d6 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 0, added: true}
    
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{"datom/e": :id, name: :names})
        struct(TestPerson, struct_map)
      end
    
    initial_map = EntityMap.new([d1, d2, d3, d4, d5, d6], 
                    cardinality_many: :name, aggregator: aggregator)
  
    d7 = %Datom{e: 0, a: :name, v: nil, tx: 1, added: true}
    d8 = %Datom{e: 1, a: :name, v: "Karen Jones", tx: 1, added: true}
    d9 = %Datom{e: 1, a: :name, v: nil, tx: 1, added: true}
    d10 = %Datom{e: 2, a: :name, v: nil, tx: 1, added: true}
    
    result = EntityMap.update(initial_map, [d7, d8, d9, d10])
    
    expected_inner_map = %{
      0 => %TestPerson{id: 0, names: MapSet.new(), age: 32},
      1 => %TestPerson{id: 1, names: MapSet.new(), age: 64}
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "In adding a value to an EntityMap containing structs, when the attribute is 
        cardinality many, a new collection value is added to the existing collection." do
    d1 = %Datom{e: 0, a: :name, v: "Bill Smith", tx: 0, added: true}
    d2 = %Datom{e: 0, a: :age, v: 32, tx: 0, added: true}
    d3 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: true}
    d4 = %Datom{e: 1, a: :name, v: "Karina Jones", tx: 0, added: true}
    
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{"datom/e": :id, name: :names})
        struct(TestPerson, struct_map)
      end
  
    initial_map = EntityMap.new([d1, d2, d3, d4], 
                    cardinality_many: :name, aggregator: aggregator)
  
    d5 = %Datom{e: 0, a: :name, v: ["Billy Smith", "B. Smith"], tx: 1, added: true}
    d6 = %Datom{e: 1, a: :name, v: MapSet.new(["Karen Jones"]), tx: 1, added: true}
    
    result = EntityMap.update(initial_map, [d5, d6])
    
    expected_inner_map = %{
      0 => %TestPerson{id: 0, names: MapSet.new(["Bill Smith", "William Smith", "Billy Smith", "B. Smith"]), age: 32},
      1 => %TestPerson{id: 1, names: MapSet.new(["Karina Jones", "Karen Jones"]), age: nil}
    }
    
    assert result.inner_map == expected_inner_map 
  end
  
  test "updates an EntityMap with records" do
    d1 = %{id: 1, attr1: [:value1, :value1a]}
    d2 = %{id: 2, attr2: [:value2]}
    
    initial_map = EntityMap.from_records([d1, d2], :id, cardinality_many: [:attr1])
    
    d5 = %{id: 1, attr1: []}
    d6 = %{id: 2, attr2: :value2a}
    
    result = EntityMap.update_from_records(initial_map, [d5, d6], :id)
    
    expected_inner_map = %{
      1 => %{"datom/e": 1, id: 1},
      2 => %{"datom/e": 2, id: 2, attr2: :value2a},
    }
    
    assert result.index_by == nil
    assert result.cardinality_many == MapSet.new([:attr1])
    assert result.inner_map == expected_inner_map
  end
    
  test "updating an EntityMap with records preserves indexing and aggregation" do
    d1 = %{eid: 1, unique_name: :bill_smith , name: "Bill Smith", age: 32}
    d2 = %{eid: 1, unique_name: :bill_smith, name: "William Smith", age: 32}
    d3 = %{eid: 2, unique_name: :karina_jones, name: ["Karina Jones", "Karen Jones"], age: 64}
    d4 = %{eid: 3, unique_name: :jim_stewart, name: "Jim Stewart", age: 23}
    d5 = %{eid: 4, unique_name: :hartley_stewart, name: MapSet.new(["Hartley Stewart"]), age: 44}
    
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{unique_name: :id, name: :names})
        struct(TestPerson, struct_map)
      end
      
    initial_map = EntityMap.from_records([d1, d2, d3, d4, d5], :eid, 
                    cardinality_many: [:name], index_by: :id, aggregator: aggregator)
  
    d6 = %{eid: 1, unique_name: :bill_smith , name: "Bill Smith", age: 33}
    d7 = %{eid: 2, unique_name: :karina_jones, name: MapSet.new(["Karen Jones"]), age: 64}
    d8 = %{eid: 3, unique_name: nil, name: [], age: nil}
    d9 = %{eid: 4, unique_name: :hartley_stewart, name: ["Hartley Stewart", "H. Stewart"], age: 44}
    
    expected_inner_map = %{
      :bill_smith => %TestPerson{id: :bill_smith, names: MapSet.new(["Bill Smith"]), age: 33},
      :karina_jones => %TestPerson{id: :karina_jones, names: MapSet.new(["Karen Jones"]), age: 64},
      :hartley_stewart => %TestPerson{id: :hartley_stewart, names: MapSet.new(["Hartley Stewart", "H. Stewart"]), age: 44}
    }
    
    result = EntityMap.update_from_records(initial_map, [d6, d7, d8, d9], :eid)
    
    assert result.index_by == :id
    assert result.cardinality_many == MapSet.new([:name])
    assert result.inner_map == expected_inner_map
  end
  
  test "updating an EntityMap with rows preserves indexing and aggregation" do
    header = [:eid, :unique_name, :name, :age]
    
    d1 = [1, :bill_smith, "Bill Smith", 32]
    d2 = [1, :bill_smith, "William Smith", 32]
    d3 = [2, :karina_jones, ["Karina Jones", "Karen Jones"], 64]
    d4 = [3, :jim_stewart, "Jim Stewart", 23]
    d5 = [4, :hartley_stewart, MapSet.new(["Hartley Stewart"]), 44]
    
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{unique_name: :id, name: :names})
        struct(TestPerson, struct_map)
      end
      
    initial_map = EntityMap.from_rows([d1, d2, d3, d4, d5], header, :eid, 
                    cardinality_many: [:name], index_by: :id, aggregator: aggregator)
  
    d6 = [1, :bill_smith, "Bill Smith", 33]
    d7 = [2, :karina_jones, MapSet.new(["Karen Jones"]), 64]
    d8 = [3, nil, [], nil]
    d9 = [4, :hartley_stewart, ["Hartley Stewart", "H. Stewart"], 44]
    
    expected_inner_map = %{
      :bill_smith => %TestPerson{id: :bill_smith, names: MapSet.new(["Bill Smith"]), age: 33},
      :karina_jones => %TestPerson{id: :karina_jones, names: MapSet.new(["Karen Jones"]), age: 64},
      :hartley_stewart => %TestPerson{id: :hartley_stewart, names: MapSet.new(["Hartley Stewart", "H. Stewart"]), age: 44}
    }
    
    result = EntityMap.update_from_rows(initial_map, [d6, d7, d8, d9], header, :eid)
    
    assert result.index_by == :id
    assert result.cardinality_many == MapSet.new([:name])
    assert result.inner_map == expected_inner_map
  end
  
  test "updates an EntityMap with a transaction, preserving indexing and aggregation" do
    d1 = %{eid: 0, identifier: :bill_smith , name: "Bill Smith", age: 32}
    d2 = %{eid: 0, identifier: :bill_smith, name: "William Smith", age: 32}
    d3 = %{eid: 1, identifier: :karina_jones, name: ["Karina Jones", "Karen Jones"], age: 64}
    d4 = %{eid: 2, identifier: :jim_stewart, name: "Jim Stewart", age: 23}
    
    aggregator = 
      fn(attr_map) -> 
        struct_map = EntityMap.rename_keys(attr_map, %{identifier: :id, name: :names})
        struct(TestPerson, struct_map)
      end
      
    initial_map = EntityMap.from_records([d1, d2, d3, d4], :eid, 
                    cardinality_many: [:name], index_by: :id, aggregator: aggregator)

    d5 = %Datom{e: 0, a: :name, v: "William Smith", tx: 0, added: false}
    d6 = %Datom{e: 1, a: :name, v: "Karen Jones", tx: 0, added: false}
    d7 = %Datom{e: 2, a: :age, v: 23, tx: 0, added: false}
    d8 = %Datom{e: 2, a: :identifier, v: nil, tx: 0, added: false}
    d9 = %Datom{e: 2, a: :name, v: "Jim Stewart", tx: 0, added: false}

    d10 = %Datom{e: 1, a: :name, v: "K. Jones", tx: 0, added: true}
    d11 = %Datom{e: 3, a: :name, v: "Hartley Stewart", tx: 0, added: true}
    d12 = %Datom{e: 3, a: :name, v: "H. Stewart", tx: 0, added: true}
    d13 = %Datom{e: 3, a: :age, v: 44, tx: 0, added: true}
    d14 = %Datom{e: 3, a: :identifier, v: :hartley_stewart, tx: 0, added: true}
    
    transaction = %DatomicTransaction{
      basis_t_before: 1000, 
      basis_t_after: 1001, 
      retracted_datoms: [d5, d6, d7, d8, d9], 
      added_datoms: [d10, d11, d12, d13, d14], 
      tempids: %{-1432323 => 64}
    }
      
    result = EntityMap.update_from_transaction(initial_map, transaction)
  
    expected_inner_map = %{
      :bill_smith => %TestPerson{id: :bill_smith, names: MapSet.new(["Bill Smith"]), age: 32},
      :karina_jones => %TestPerson{id: :karina_jones, names: MapSet.new(["Karina Jones", "K. Jones"]), age: 64},
      :hartley_stewart => %TestPerson{id: :hartley_stewart, names: MapSet.new(["Hartley Stewart", "H. Stewart"]), age: 44}
    }
    
    assert result.inner_map == expected_inner_map
    assert result.index_by == :id
    assert result.cardinality_many == MapSet.new([:name])
    assert result.aggregator == aggregator
  end
end
