defmodule DatomicGenServer.EntityMap do
  @moduledoc """
      `DatomicGenServer.EntityMap` is a data structure designed to store the results
      of Datomic queries and transactions in a map of maps or structs. The keys
      of the `EntityMap` are by default Datomic entity IDs, but you may also index
      the map using other attributes as keys.
      
      An `EntityMap` may be created from data tuples, records, or rows. A `DataTuple`
      is a generalization of `Datom` which contains `e`, `a`, `v`, and `added` 
      fields, where `v` may be a scalar value or a collection. A record in this
      context is simply a map of attributes to values. A row is a list of 
      attribute values, which must be accompanied by a "header row" containing
      a list of attribute keys in the same list position as their corresponding
      attribute values -- i.e., the first key in the header is the attribute key
      for the first value in each of the rows, etc.
      
      The main functions for creting an `EntityMap` are `new` (to create from
      data tuples), `from_records`, `from_rows`, and `from_transaction` (to 
      create from a Datomic transaction). An `EntityMap` may be updated using 
      `update` (from data tuples), `update_from_records`, `update_from_rows`,
      and `update_from_transaction`. An entity may be retrieved using `get`, and
      a single attribute from an entity using `get_attr`.
      
      When creating an `EntityMap` you may specify the attribute key to index by
      and the struct into which to place attribute values (along with a translation
      table for mapping the keys in the incoming data to any fields in the struct
      that might have different names). You must also specify any cardinality many 
      attributes; these values are contained in sets: If a data tuple contains a
      scalar value for a cardinality many attribute, then that value is added to
      the set of values for that attribute (or removed, in the case of retraction).
      If a data tuple contains a collection as a value for a cardinality many 
      attribute, the values are added to or removed from the set of values for
      that attribute.
      
      When working with records and rows, the behavior is different: The value 
      for a cardinality many attribute in a row or record should always be a
      collection, and will always replace any pre-existing set of attributes.
      In other words, using records and rows always assumes that a single record
      or row contains the entire accumulated value for an attribute. There are no
      separate addition and retraction operations for records and rows; any
      attribute values in a record or row replace prior values. If a record or
      row does not contain an entry for a particular attribute, that attribute
      is left in its prior state.
      
      Empty collections and `nil` are "magic values" with a special behavior. 
      When working with data tuples, either adding or retracting `nil` from a
      scalar attribute deletes the key for that attribute from the attribute map.
      Adding or retracting an empty collection (list or set) from a cardinality
      many attribute resets that value to the empty set. If you want to completely
      replace a set of values for a cardinality many attribute with a different
      set, you would first retract `nil` for that attribute and then add the new
      attribute values. Note that `nil` and the empty collection always override
      any other values provided for the same operation (add or retract) for
      that attribute in a group of data tuples. In other words, if you retract
      a particular value and also retract `nil` in the same collection of 
      data tuples, the `nil` wins; the same would go for adding `nil` along with
      other values. (You can, however, retract `nil` and at the same time add
      new values.)
      
      When working with records and rows, if you supply `nil` or an empty 
      collection as a value for an attribute, that value is removed from
      the attribute map (or set to the empty set if the attribute is cardinality
      many).
      
      Note that internally, `EntityMap` uses an empty tuple as a null value marker. 
      You should not use empty tuples in attribute values.
            
      Empty entities -- entities with no attribute values other than empty 
      collections and the entity ID -- are removed from the entity map.

## Aggregation

      You can specify a struct to carry attribute values, along with a 
      translation from the attribute keys in the raw data to the names of the
      fields in the struct. Note that it is possible for an EntityMap to contain 
      entities of different types, whereas an aggregator will narrow the 
      EntityMap to entities of a single type -- attribute maps that cannot be
      aggregated into the struct will not appear in the aggregated map. Note,
      however, that to be able to be aggregated into a struct, an attribute map
      only needs to share one key with that struct -- any attributes that the
      map has that are not in the struct are discarded, and any fields of the
      struct that are not present in the attribute map are set to their default
      values. Note also that if you supply a translation table for attribute keys
      to struct fields, if an attribute map already has a key with the same name
      as a struct field, that value will still be mapped into the struct -- i.e.,
      if you provide a translation table that maps the attribute key `:identifier`
      to `:id`, then any attribute values with the key `:identifier` will be put 
      into the struct's `id` field--but so will any attribute values with the key 
      `:id`.
      
      Since any entities failing aggregation aren't included in the aggregated
      map, you can use aggregators to filter an `EntityMap` to contain just those
      entities that you want. (See e.g., the `aggregate_by` function, which
      returns a new `EntityMap` containing the original data aggregated in a
      new way.) This also allows you to create multiple `EntityMaps` from a 
      single original `EntityMap`, each of which has an aggregated map that
      references only certain entities in the data.
      
      Internally, the `EntityMap` still contains all its original data,
      so if you re-aggregate an already aggregated map, the new aggregation is
      applied to all the data used to construct the original map, and not just 
      to the data accessible in the already-aggregated map.
      
## Indexing

      You can choose an attribute that will supply the keys for the `EntityMap`.
      If the raw data for an entity does not contain a value for that attribute,
      that entity will not appear in the indexed map.
      
      If you are indexing an aggregated map, the possible keys are the names of 
      the struct fields, not the names of the raw attributes those fields have
      been translated from.
"""  
  defstruct raw_data: %{}, inner_map: %{}, 
            cardinality_many: MapSet.new(), index_by: nil, 
            aggregator: nil, aggregate_field_to_raw_attribute: %{}
  @type t :: %DatomicGenServer.EntityMap{
    raw_data: map, inner_map: map, 
    cardinality_many: MapSet.t, index_by: term, 
    aggregator: (DataTuple.t -> term), aggregate_field_to_raw_attribute: map
  }
  
  defmodule DataTuple do
    defstruct e: nil, a: nil, v: nil, added: false
    @type t :: %DataTuple{e: term, a: term, v: term, added: boolean}
  end
  
  @spec e_key :: :"datom/e"
  def e_key, do: :"datom/e"
  
  @type aggregate :: {module, map}
  @type entity_map_option :: {atom, term} | {atom, MapSet.t} | {atom, aggregate}
  
  @spec set_defaults([entity_map_option]) :: [entity_map_option]
  def set_defaults(options) do
    [ index_by: options[:index_by],
      cardinality_many: to_set(options[:cardinality_many]),
      aggregator: to_aggregator(options[:aggregate_into]),
      aggregate_field_to_raw_attribute: invert_attribute_translation_map(options[:aggregate_into])
    ]
  end
  
  defp to_set(one_or_many) do
    case one_or_many do 
      %MapSet{} -> one_or_many
      [_] -> MapSet.new(one_or_many)
      [] -> MapSet.new()
      nil -> MapSet.new()
      _ -> MapSet.new([one_or_many])
    end
  end
  
  defp to_aggregator(aggregator_pair) do
    case aggregator_pair do
      {aggregate_struct, key_rename_map} ->
        fn(attr_map) -> 
          struct_map = rename_keys(attr_map, key_rename_map)
          struct(aggregate_struct, struct_map)
        end
      _ -> fn(x) -> x end
    end
  end
  
  defp invert_attribute_translation_map(aggregator_pair) do
    case aggregator_pair do
      {_, key_rename_map} ->
        key_rename_map
        |> Enum.map(fn({k,v}) -> {v, k} end)
        |> Enum.into(%{})
      _ -> nil
    end
  end

  @doc """
  Create a new `EntityMap` from a list of `DataTuple`s. An `EntityMap` acts as a
  map of an entity id (or attribute value) to a map of an entity's attributes.
  
  The supplied data tuples may have the value `true` or `false` for the `added`
  field, but tuples with a false value are ignored. Note that the incoming 
  data tuples may include data for multiple different types of entities.
  
  By default, if the attributes are not aggregated into a struct, the attribute
  map will contain an extra field :"datom/e" whose value is the entity ID.
  
  The following options are supported:
  
  `:cardinality_many` - the name or names of attribute keys that correspond to
  `cardinality/many` attributes. If you have such attributes in your data, this
  option is required. The value for this option may be a single value, a list, or
  a set. The name should be the name of the attribute on the _incoming_ data,
  irrespective of any aggregation. If a data tuple contains a scalar value for a 
  cardinality many attribute, then that value is added to the set of values for t
  hat attribute. If a data tuple contains a collection as a value for a cardinality 
  many attribute, the values are added to the set of values for that attribute. 
  Note that if a cardinality many attribute is not present in the data tuples for 
  an entity, it will not be present in the resulting attribute map; trying to get 
  its value won't give you an empty set; it will give you null. 

  `:aggregate_into` - this should be a pair, the first element of which is a
  module (i.e., the struct you wish to use to aggregate results) and the second
  of which is a map from keys in the raw data to fields of the struct. It is 
  not necessary to map keys that have the same name as fields in the struct, but
  only keys that need to be translated. The aggregator is stored with the entity 
  map; it is assumed that all the DataTuples or records that you will be adding 
  or removing later have the same relevant attributes and so will be aggregated  
  the same way.
  
  `:index_by` - if you wish to use something other than the entity ID as the key
  for the `EntityMap`, specify the attribute name here. If you are aggregating
  the map into a struct, this should be the name of the field in the struct
  rather than the name of the attribute key in the data used to construct the
  `EntityMap`.

## Example
  
      d1 = %Datom{e: 0, a: :attr1, v: :value, tx: 0, added: true}
      d2 = %Datom{e: 0, a: :attr2, v: :value2, tx: 0, added: true}
      d3 = %Datom{e: 1, a: :attr2, v: :value3, tx: 0, added: true}
      d4 = %Datom{e: 1, a: :attr3, v: :value2, tx: 0, added: false}

      entity_map = EntityMap.new([d1, d2, d3, d4, d5, d6])

      EntityMap.get_attr(entity_map, 1, :attr2)

      => :value3

  """
  @spec new([DataTuple.t], [entity_map_option]) :: EntityMap.t
  def new(data_tuples_to_add \\ [], options \\ []) do
    opts = set_defaults(options)

    raw_data_map = 
      data_tuples_to_add
      |> Enum.filter(fn(data_tuple) -> data_tuple.added end)
      |> to_raw_data_map_with_null_markers(opts[:cardinality_many])
      |> filter_null_attributes(opts[:cardinality_many])
      |> filter_null_entities
      
    inner_map =
      raw_data_map
      |> to_aggregated_map(opts[:aggregator])
      |> index_if_necessary(opts[:index_by])
    
    %__MODULE__{raw_data: raw_data_map,
                inner_map: inner_map, 
                index_by: opts[:index_by], 
                cardinality_many: opts[:cardinality_many], 
                aggregator: opts[:aggregator],
                aggregate_field_to_raw_attribute: opts[:aggregate_field_to_raw_attribute]
              }
  end
  
  
  # We use an empty tuple as a null marker. Otherwise, we cannot distinguish
  # between a key not being in the map when we are adding new values, and a value
  # which is being set to null when a group of data tuples is being passed in.
  # This makes sure the null carries through if many values are being added for 
  # a given attribute, only one of which is null.
  # If there is a pre-existing value for an attribute, and it is the empty tuple,
  # then that value remains the empty tuple, regardless of the incoming value.
  # If there is no value for an attribute, and the incoming value is nil, an
  # empty set, or an empty list, then that value is set to the empty tuple.
  # (DataTuples, unlike Datoms, may have collections as values for attributes.)
  @spec to_raw_data_map_with_null_markers([DataTuple.t], MapSet.t) :: map
  defp to_raw_data_map_with_null_markers(data_tuples, cardinality_set) do
    List.foldl(data_tuples, %{}, 
      fn(data_tuple, accumulator) -> 
        updated_record = 
          if existing_entity_attributes = Map.get(accumulator, data_tuple.e) do
            add_to_attribute_map(existing_entity_attributes, data_tuple.a, data_tuple.v, cardinality_set)
          else
            new_attribute_map(data_tuple.e, data_tuple.a, data_tuple.v, cardinality_set)
          end
        Map.put(accumulator, data_tuple.e, updated_record)
      end)    
  end

  @spec new_attribute_map(term, term, term, MapSet.t) :: map
  defp new_attribute_map(entity_id, attr, value, cardinality_many) do
    new_map = add_to_attribute_map(%{}, attr, value, cardinality_many)
    add_to_attribute_map(new_map, e_key, entity_id, cardinality_many) 
  end

  # If we have multiple DataTuples, each with a scalar value for a cardinality many
  # attribute, we add each new value to the set of prior values. In a DataTuple,
  # unlike with a simple datom, we might get a collection as a value. In that 
  # case, since we are constructing a new EntityMap here, we shouldn't get more
  # than one DataTuple for that attribute of that entity. If by chance we do, 
  # we union it with the previous value.
  @spec add_to_attribute_map(map, term, term, MapSet.t) :: map
  defp add_to_attribute_map(attr_map, attr, value, cardinality_many) do
    old_value = initialize_if_needed(attr, Map.get(attr_map, attr), cardinality_many)
    new_value = updated_attribute_value(old_value, with_null_marker(value))
    
    # We don't filter out the null markers here because they need to carry through
    # the accumulated value and prevent additional attribute values from being added.
    Map.put(attr_map, attr, new_value)  
  end
  
  @spec initialize_if_needed(term, term, MapSet.t) :: term
  defp initialize_if_needed(attr, prior_attribute_value, cardinality_many) do
    if prior_attribute_value do
      prior_attribute_value
    else
      if MapSet.member?(cardinality_many, attr) do MapSet.new() else nil end
    end
  end
  
  @spec with_null_marker(term) :: term
  defp with_null_marker(value) do
    if is_null_value(value) do {} else value end
  end
  
  @spec is_null_value(term) :: boolean
  defp is_null_value(value) do
    case value do
      nil -> true
      [] -> true
      %MapSet{} -> if MapSet.size(value) == 0 do true else false end
      _ -> false
    end
  end
  
  @spec updated_attribute_value(term, term) :: term
  defp updated_attribute_value(old_value, new_value) do
    case {old_value, new_value} do
      {{}, _} -> {}
      {_, {}} -> {}
      {%MapSet{}, %MapSet{}} -> MapSet.union(old_value, new_value)
      {%MapSet{}, [_|_]} -> MapSet.union(old_value, MapSet.new(new_value))
      {%MapSet{}, _} -> MapSet.put(old_value, new_value)    
      _ -> new_value
    end
  end
  
  @spec filter_null_attributes(map, MapSet.t) :: map
  defp filter_null_attributes(entity_id_to_attributes, cardinality_many) do 
    null_cardinality_many_attribute_to_empty_set = 
      fn({attr, val}) -> 
        case val do 
          {} -> if MapSet.member?(cardinality_many, attr) do {attr, MapSet.new()} else {attr, val} end
          _ -> {attr, val}
        end
      end
    
    missing_cardinality_many_attribute_to_empty_set =
      fn(attr_map) ->
        cardinality_many
        |> MapSet.to_list
        |> List.foldl(attr_map, fn(attr, accum) -> 
            if Map.get(accum, attr) do accum else Map.put(accum, attr, MapSet.new()) end 
           end)
      end
      
    is_non_null_attribute = fn({_, val}) -> val != {} end

    handle_null_attributes = 
      fn(attr_map) ->
        attr_map
        |> missing_cardinality_many_attribute_to_empty_set.()
        |> Enum.map((&null_cardinality_many_attribute_to_empty_set.(&1)))
        |> Enum.filter((&is_non_null_attribute.(&1)))
        |> Enum.into(%{})
      end

    entity_id_to_attributes |> Enum.map(
      fn({entity_id, attr_map}) -> 
        { entity_id, handle_null_attributes.(attr_map) } 
      end)
  end
  
  @spec filter_null_entities(map) :: map
  defp filter_null_entities(entity_id_to_attributes) do 
    entity_id_to_attributes
    |> Enum.filter(fn({_, attr_map}) -> ! empty_entity?(attr_map) end)
    |> Enum.into(%{})  
  end
  
  @spec empty_entity?(map) :: boolean
  defp empty_entity?(attr_map) do
    attr_map
    |> Map.delete(e_key)
    |> Enum.filter(fn({_, value}) ->     
        case value do
          %MapSet{} -> if MapSet.size(value) == 0 do false else true end
          _ -> true
        end
       end)
    |> (fn(keyword_list) -> Enum.empty?(keyword_list) end).()
  end
  
  @spec to_aggregated_map(map, (DataTuple.t -> term)) :: map
  defp to_aggregated_map(entities_by_id, aggregator) do
    entities_by_id
    |> Enum.map(fn({entity_id, attr_map}) -> {entity_id, aggregator.(attr_map)} end)
    |> Enum.into(%{})  
  end
  
  @spec index_if_necessary(map, term) :: map
  defp index_if_necessary(entity_mapping, attribute) do
    if attribute do 
      index_map_by(entity_mapping, attribute) 
    else 
      entity_mapping 
    end
  end

  @doc """
  Create a new `EntityMap` from a list of records. A record is a map of attribute 
  names to values. 
  
  One of those attribute keys must point to the entity's primary key -- i.e., 
  the same value that would appearin the `e` value of a corresponding datom or 
  `DataTuple`. This key should be supplied as the second parameter to the function.
  By default, if the `EntityMap` does not aggregate the attributes into a struct, 
  the attribute map will contain an extra field :"datom/e" whose value is the entity 
  ID.
  
  Note that the incoming records may include data for multiple different types of 
  entities.
  
  The following options are supported:
  
  `:cardinality_many` - the name or names of attribute keys that correspond to
  `cardinality/many` attributes. If you have such attributes in your data, this
  option is required. The value for this option may be a single value, a list, or
  a set. The name should be the name of the attribute on the _incoming_ record,
  irrespective of any aggregation. The value for a cardinality many attribute
  on an incoming record should be a set or a list.

  `:aggregate_into` - this should be a pair, the first element of which is a
  module (i.e., the struct you wish to use to aggregate results) and the second
  of which is a map from keys in the record to fields of the struct. It is 
  not necessary to map keys that have the same name as fields in the struct, but
  only keys that need to be translated. The aggregator is stored with the entity 
  map; it is assumed that all the DataTuples or records that you will be adding 
  or removing later have the same relevant attributes and so will be aggregated
  the same way.
  
  `:index_by` - if you wish to use something other than the entity ID as the key
  for the `EntityMap`, specify the attribute name here. If you are aggregating
  the map into a struct, this should be the name of the field in the struct
  rather than the name of the attribute key in the record used to construct the
  `EntityMap`.

  ## Example
        d1 = %{eid: 1, unique_name: :bill_smith, name: "Bill Smith", age: 32}
        d2 = %{eid: 2, unique_name: :karina_jones, name: "Karina Jones", age: 64}
        
        result_struct = {TestPerson, %{unique_name: :id, name: :names}}
          
        entity_map = EntityMap.from_records([d1, d2], :eid,  
                      cardinality_many: [:name], 
                      index_by: :id, 
                      aggregate_into: result_struct)

        EntityMap.get(entity_map, :karina_jones)

          => %TestPerson{id: :karina_jones, names: MapSet.new(["Karina Jones"]), age: 64}

  """  
  @spec from_records([map] | MapSet.t, term, [entity_map_option]) :: EntityMap.t
  def from_records(record_maps_to_add, primary_key, options \\ []) do
    
    record_maps_to_add 
    |> Enum.map(fn(attr_map) -> 
        entity_id = Map.get(attr_map, primary_key)
        Enum.map(attr_map, fn{attr, value} -> 
           %DataTuple{e: entity_id, a: attr, v: value, added: true}
         end)
       end)
    |> Enum.concat
    |> new(options)
  end

  
  @doc """
  Create a new `EntityMap` from a list of rows. A row is a simple list of 
  attribute values. The header (second parameter) supplies a list of the attribute 
  names for these values.
  
  One of those attribute keys must point to the entity's primary key -- i.e., 
  the same value that would appearin the `e` value of a corresponding datom or 
  `DataTuple`. This key should be supplied as the third parameter to the function.
  By default, if the `EntityMap` does not aggregate the attributes into a struct, 
  the attribute map will contain an extra field :"datom/e" whose value is the entity 
  ID.
  
  Note that the incoming records may include data for multiple different types of 
  entities.
  
  The following options are supported:
  
  `:cardinality_many` - the name or names of attribute keys that correspond to
  `cardinality/many` attributes. If you have such attributes in your data, this
  option is required. The value for this option may be a single value, a list, or
  a set. The name should be the name of the attribute on the _incoming_ row header,
  irrespective of any aggregation. The value for a cardinality many attribute
  on an incoming row should be a set or a list.

  `:aggregate_into` - this should be a pair, the first element of which is a
  module (i.e., the struct you wish to use to aggregate results) and the second
  of which is a map from keys in the record to fields of the struct. It is 
  not necessary to map keys that have the same name as fields in the struct, but
  only keys that need to be translated. The aggregator is stored with the entity 
  map; it is assumed that all the DataTuples or records that you will be adding 
  or removing later have the same relevant attributes and so will be aggregated
  the same way.
  
  `:index_by` - if you wish to use something other than the entity ID as the key
  for the `EntityMap`, specify the attribute name here. If you are aggregating
  the map into a struct, this should be the name of the field in the struct
  rather than the name of the attribute key in the header.

  ## Example
  
        header = [:eid, :unique_name, :name, :age]

        d1 = [1, :bill_smith, "Bill Smith", 32]
        d2 = [2, :karina_jones, "Karina Jones", 64]
        
        result_struct = {TestPerson, %{unique_name: :id, name: :names}}
          
        entity_map = EntityMap.from_rows([d1, d2], header, :eid,  
                      cardinality_many: [:name], 
                      index_by: :id, 
                      aggregate_into: result_struct)

        EntityMap.get(entity_map, :karina_jones)

          => %TestPerson{id: :karina_jones, names: MapSet.new(["Karina Jones"]), age: 64}

  """  
  @spec from_rows([list] | MapSet.t, list, term, [entity_map_option]) :: EntityMap.t
  def from_rows(rows_to_add, header, primary_key, options \\ []) do
    rows_to_add
    |> rows_to_records(header)
    |> from_records(primary_key, options)    
  end
  
  @spec rows_to_records([list] | MapSet.t, list) :: [map]
  defp rows_to_records(rows, header) do
    Enum.map(rows, fn(row) -> Enum.zip(header, row) |> Enum.into(%{})  end)
  end
  
  @spec from_transaction(DatomicTransaction.t, [entity_map_option]) :: EntityMap.t
  def from_transaction(transaction, options \\ []) do
    new(transaction.added_datoms, options)
  end
  
  # NOTE - INDEXING HAPPENS ON THE ATTRIBUTE IN THE STRUCT, NOT IN THE INCOMING
  # ATTRIBUTES. CARDINALITY MANY IS A NAME OF AN INCOMING ATTRIBUTE
  @spec update(EntityMap.t, [DataTuple.t]) :: EntityMap.t
  def update(entity_map, data_tuples_to_update) do
    {data_tuples_to_add, data_tuples_to_retract} = 
      Enum.partition(data_tuples_to_update, fn(data_tuple) -> data_tuple.added end)

    raw_data_map_with_retractions =
      data_tuples_to_retract
      |> to_raw_data_map_with_null_markers(entity_map.cardinality_many)
      |> Enum.reduce(entity_map.raw_data, &retract/2)
      |> filter_null_attributes(entity_map.cardinality_many)
      |> filter_null_entities

    raw_data_map_with_all_changes =
      data_tuples_to_add
      |> to_raw_data_map_with_null_markers(entity_map.cardinality_many)
      |> Enum.reduce(raw_data_map_with_retractions, &merge_with_new_values/2)
      |> filter_null_attributes(entity_map.cardinality_many)
      |> filter_null_entities

    updated_inner_map =
      raw_data_map_with_all_changes
      |> to_aggregated_map(entity_map.aggregator)
      |> index_if_necessary(entity_map.index_by)
    
    %__MODULE__{raw_data: raw_data_map_with_all_changes,
                inner_map: updated_inner_map, 
                index_by: entity_map.index_by, 
                cardinality_many: entity_map.cardinality_many, 
                aggregator: entity_map.aggregator,
                aggregate_field_to_raw_attribute: entity_map.aggregate_field_to_raw_attribute
               }
  end
    
  @spec retract({term, map}, map) :: map
  defp retract({entity_id, attributes_to_retract}, entity_map) do
    existing_attributes = Map.get(entity_map, entity_id) || %{}
    
    new_attr_map = 
      attributes_to_retract
      |> Enum.reduce(existing_attributes, &remove_attribute_value/2)

    Map.put(entity_map, entity_id, new_attr_map)
  end
  
  # When retracting from an attribute with cardinality many, the value has
  # to be removed from the Set of values if it's one value, or the set of values
  # subtracted if it's a set of values.
  # Passing in a nil value or an empty collection results in any pre-existing value
  # for that attribute being retracted.
    
  # Will only retract the value if the map contains that value for that attribute.
  # For maps, regardless of the current value of the attribute, you can also
  # pass in a nil value or an empty collection to to remove the attribute key from 
  # the map. Datomic datoms won't ever come back with nil or [] values so it's
  # ok to use these in a struct to signify empty values, since there's never nils
  # in the database. (This is mainly for use with records.)
  # When retracting from an attribute with cardinality many, the value is
  # removed from the Set of values if it's one value, or the set of values
  # subtracted if it's a set of values.
  # If the :"datom/e" key is in the map, we don't remove it. Other indexed
  # attributes will be removed if they are in the map to be retracted.
  defp remove_attribute_value({attr, value}, attr_map) do
    attribute_is_datom_e = attr == e_key
    old_value = Map.get(attr_map, attr)
    # If the value to remove is nil, it has already been changed to the null marker {}.
    case {old_value, value} do
      {_, _} when attribute_is_datom_e ->
        attr_map    
      {_, {}} ->
        Map.put(attr_map, attr, {})
      {old, val} when val == old -> 
        Map.delete(attr_map, attr)
      {%MapSet{}, %MapSet{}} ->
        Map.put(attr_map, attr, MapSet.difference(old_value, value))
      {%MapSet{}, [_|_]} -> 
        Map.put(attr_map, attr, MapSet.difference(old_value, MapSet.new(value)))
      {%MapSet{}, _} -> 
        Map.put(attr_map, attr, MapSet.delete(old_value, value))
      _ ->
        attr_map
    end
  end 
  
  @spec merge_with_new_values({term, map}, map) :: map
  defp merge_with_new_values({entity_id, attributes_to_add}, entity_map) do
    existing_attributes = Map.get(entity_map, entity_id) || %{}
    merge_fn = fn(_, old_value, new_value) -> updated_attribute_value(old_value, new_value) end
    new_attr_map = Map.merge(existing_attributes, attributes_to_add, merge_fn)
    Map.put(entity_map, entity_id, new_attr_map)
  end

  # A record is a map of attribute names to values. One of those attribute keys 
  # must point to the entity primary key -- i.e., the same value that would appear
  # in the `e` value of a corresponding datom or DataTuple. This attribute should be passed to 
  # the function as the third argument.
  # The records will be converted to DataTuples with the e: field being the value
  # of the entity identifier. The update function taking a DataTuple list will then
  # be called with the result.
  # NOTE For cardinality many attributes you have two choices:
  # 1) You may have a set or list as the value of the attribute in the record map
  # 2) You may have multiple records with individual values or partial collections
  # IN EITHER CASE the final attribute value will be the union of all the 
  # attribute values passed in, and it will REPLACE any existing value for that
  # attribute. In other words, you have to have the complete collection of values
  # for that attribute; there is no way to partially update a cardinality many 
  # attribute value using record syntax.
  # NOTE - INDEXING HAPPENS ON THE ATTRIBUTE IN THE STRUCT, NOT IN THE INCOMING
  # ATTRIBUTES. CARDINALITY MANY IS A NAME OF AN INCOMING ATTRIBUTE
  @spec update_from_records(EntityMap.t, Enum.t, term) :: EntityMap.t
  def update_from_records(entity_map, record_maps, primary_key) do

    data_tuples =
      record_maps 
      |> Enum.map(fn(attr_map) -> 
          entity_id = Map.get(attr_map, primary_key)
          Enum.map(attr_map, fn{attr, value} -> 
             [%DataTuple{e: entity_id, a: attr, v: value, added: true},
              # Retract any prior value for that attribute on that entity.
              %DataTuple{e: entity_id, a: attr, v: nil, added: false}
             ]
           end)
         end)
      # Each attr-value pair generates a pair of data tuples.
      # This makes for a list of pairs of tuples by entity
      # And so for all the entities together there's an outer list of all of those.
      |> Enum.concat  
      |> Enum.concat
    update(entity_map, data_tuples)
  end
  
  @spec update_from_rows(EntityMap.t, [list] | MapSet.t, list, term) :: EntityMap.t
  def update_from_rows(entity_map, rows_to_add, header, primary_key) do
    records_to_add = rows_to_records(rows_to_add, header)
    update_from_records(entity_map, records_to_add, primary_key)
  end
  
  @spec update_from_transaction(EntityMap.t, DatomicTransaction.t) :: EntityMap.t
  def update_from_transaction(entity_map, transaction) do
    all_datoms = transaction.retracted_datoms ++ transaction.added_datoms
    update(entity_map, all_datoms)
  end
  
  # Map functions
  
  # Make sure that the index exists in the new aggregate! Defaults to the old
  # index.
  @spec aggregate_by(EntityMap.t, aggregate, term) :: EntityMap.t
  def aggregate_by(entity_map, aggregate, new_index \\ nil) do
    new_aggregator = to_aggregator(aggregate)
    new_aggregate_field_to_raw_attribute = invert_attribute_translation_map(aggregate)

    new_inner_map = 
      entity_map.raw_data
      |> to_aggregated_map(new_aggregator)
      |> index_if_necessary(new_index || entity_map.index_by)
      
    %__MODULE__{raw_data: entity_map.raw_data,
                inner_map: new_inner_map, 
                index_by: entity_map.index_by, 
                cardinality_many: entity_map.cardinality_many, 
                aggregator: new_aggregator,
                aggregate_field_to_raw_attribute: new_aggregate_field_to_raw_attribute
               }

  end
  
  # Deletes the entry in the EntityMap for a specific index key. If the EntityMap 
  # is not indexed, the entity ID is used. If the key does not exist, returns the 
  # map unchanged.
  @spec delete(map, term) :: EntityMap.t
  def delete(entity_map, index_key) do
    map_entry = get(entity_map, index_key)
    if map_entry do
      new_inner_map = Map.delete(entity_map.inner_map, index_key)
      
      entity_id = entity_id_from_index(entity_map, index_key)
      new_raw_data = Map.delete(entity_map.raw_data, entity_id)
      
      %__MODULE__{raw_data: new_raw_data,
                  inner_map: new_inner_map, 
                  index_by: entity_map.index_by, 
                  cardinality_many: entity_map.cardinality_many, 
                  aggregator: entity_map.aggregator,
                  aggregate_field_to_raw_attribute: entity_map.aggregate_field_to_raw_attribute
                 }
    else
      entity_map
    end
  end

  # TODO Deletes the entries for all of the given keys from the map.
  #
  # drop(entity_map, entity_keys)

  # Checks if two EntityMaps contain equal data. Their index_by and aggregator
  # are ignored.
  @spec equal?(EntityMap.t, EntityMap.t) :: boolean
  def equal?(entity_map1, entity_map2) do
    Map.equal?(entity_map1.inner_map, entity_map2.inner_map)
  end

  # TODO Fetches the value for a specific entity key and returns it in a tuple
  # If the key does not exist, returns :error.
  #
  # fetch(entity_map, entity_key)
  
  # TODO 
  #
  # fetch_attr(entity_map, entity_key, attr_key)
  
  # TODO Fetches the value for a specific entity key. If the key does not exist, 
  # a `KeyError` is raised.
  #
  # fetch!(entity_map, entity_key)
  
  # TODO 
  #
  # fetch_attr!(entity_map, entity_key, attr_key)

  # Gets the value for a specific index key. If the EntityMap is not indexed, the 
  # entity ID is used. If the key does not exist, returns the default value
  # (or nil if there is no default value).
  @spec get(EntityMap.t, term, term) :: term
  def get(entity_map, index_key, default \\ nil) do
    Map.get(entity_map.inner_map, index_key, default)
  end
  
  # Gets the value for a specific index key and attribute key. If the EntityMap 
  # is not indexed, the entity ID is used as the index. If the EntityMap is
  # aggregated, the attribute key is the name of the field in the aggregated
  # struct; the attribute names in the raw data are not used. If either key does 
  # not exist, the default value is returned (or nil if there is no default value).
  @spec get_attr(EntityMap.t, term, term, term) :: term
  def get_attr(entity_map, index_key, attr_key, default \\ nil) do
    entity = Map.get(entity_map.inner_map, index_key)
    if entity do
      Map.get(entity, attr_key, default)
    else
      default
    end
  end
  
  # Returns whether a given index key exists in the given map
  @spec has_key?(EntityMap.t, term) :: boolean
  def has_key?(entity_map, index_key) do
    Map.has_key?(entity_map.inner_map, index_key)
  end
  
  # Creates a new EntityMap whose keys are values of a certain attribute or 
  # struct field rather than the entity IDs.
  # Assumes that the value you are indexing on is unique, otherwise you will lose
  # entities. If a value for the attribute is not present in the attribute map/struct,
  # it will go into the entity map with a key of nil.
  # index_by is applied after your aggregator, so you can use the name of a field
  # in your struct. If a field in the data_tuples isn't in the struct, you can't index by it.
  @spec index_by(EntityMap.t, term) :: EntityMap.t
  def index_by(entity_map, attribute) do
    new_inner = entity_map.inner_map |> index_map_by(attribute)
      
    %__MODULE__{raw_data: entity_map.raw_data,
                inner_map: new_inner, 
                index_by: attribute, 
                cardinality_many: entity_map.cardinality_many,
                aggregator: entity_map.aggregator,
                aggregate_field_to_raw_attribute: entity_map.aggregate_field_to_raw_attribute
               }
  end
  
  # Removes nil keys, so entities that don't have the index attribute don't show
  # up in the map.
  @spec index_map_by(map, term) :: map
  defp index_map_by(entity_mapping, attribute) do
    entity_mapping
    |> Enum.map(fn({_, attributes}) -> {Map.get(attributes, attribute), attributes} end)
    |> Enum.into(%{})
    |> Map.delete(nil) 
  end
  
  @spec entity_id_from_index(EntityMap.t, term) :: term
  defp entity_id_from_index(entity_map, index_key) do
    if ! entity_map.index_by do 
      index_key
    else
      raw_data_index_attr = to_raw_attribute(entity_map, entity_map.index_by)      
      
      {entity_id, _} = 
        Enum.find(entity_map.raw_data, fn({_, attributes}) -> 
          Map.get(attributes, raw_data_index_attr) == index_key 
        end)
        
      entity_id
    end
  end
  
  @spec to_raw_attribute(EntityMap.t, term) :: term
  defp to_raw_attribute(entity_map, attr_key) do
    if entity_map.aggregate_field_to_raw_attribute do
      val = Map.get(entity_map.aggregate_field_to_raw_attribute, attr_key)
      if val do val else attr_key end
    else
      attr_key
    end
  end

  # Returns all index keys from the map, in a list.
  @spec keys(EntityMap.t) :: [term]
  def keys(entity_map) do
    Map.keys(entity_map.inner_map)
  end
  
  # TODO Returns and removes the value associated with an entity key in the map
  #
  # pop(entity_map, entity_key, default \\ nil)

  # Returns an EntityMap with the given entity added. Requires that the entity 
  # be in the form of a map of attributes to values, including the entity id 
  # (not just the index key). The key of the field containing the entity id 
  # should be passed as the third argument. If an entity already exists for that
  # ID, it will be replaced with the new one.
  @spec put(EntityMap.t, map, term) :: EntityMap.t
  def put(entity_map, record, primary_key) do
    update_from_records(entity_map, [record], primary_key)
  end
  
  # Returns an EntityMap with an updated value for a given entity and attribute.
  # If the EntityMap is not indexed, the the index key should be the entity ID. 
  # If the EntityMap is aggregated, the attribute key is the name of the field in 
  # the aggregated struct; the attribute names in the raw data are not used. 
  # If either key does not exist, ...?
  @spec put_attr(EntityMap.t, term, term, term, [{atom, boolean}]) :: {:ok, EntityMap.t} | {:error, String.t}
  def put_attr(entity_map, index_key, attr_key, val, options \\ []) do
    entity_id = entity_id_from_index(entity_map, index_key)
    raw_attr_key = to_raw_attribute(entity_map, attr_key)
    cond do
      ! entity_id -> {:error, "Unable to find entity ID for index key #{index_key}"}
      ! raw_attr_key -> {:error, "Unable to determine raw attribute key for aggregate attribute #{attr_key}"}
      true ->
        tuple_to_add = %DataTuple{e: entity_id, a: raw_attr_key, v: val, added: true}
        tuples_to_update = if options[:overwrite_collection] do
          [tuple_to_add, %DataTuple{e: entity_id, a: raw_attr_key, v: nil, added: false}]
        else
          [tuple_to_add]
        end
        new_entity_map = update(entity_map, tuples_to_update)
       {:ok, new_entity_map}
    end
  end
  
  # TODO Puts the given value under key unless the entry key already exists
  #
  # put_new(entity_map, entity_key, value)
  
  # Utility function: Pass in a map and another map from keys to new keys.
  @spec rename_keys(map, map) :: EntityMap.t
  def rename_keys(map, key_rename_map) do
    Enum.map(map, fn({k,v}) ->             
      new_name = Map.get(key_rename_map, k)
      if new_name do {new_name, v} else {k,v} end
    end)                                       
    |> Enum.into(%{})                          
  end
  
  # Returns all entities from the EntityMap
  @spec values(EntityMap.t) :: [term]
  def values(entity_map) do
    Map.values(entity_map.inner_map)
  end
  
  ####################################################################################
  
  # FUTURE POSSIBILITIES:
  
  # Allow multiple aggregators so that entities of different types can exist
  # in the same map.


  # Returns a new EntityMap containing a subset of the original EntityMap's data,
  # according to the filter function, which takes a key/aggregate pair and returns true or false.
  # An optional aggregator may be passed in to become the aggregator for the new map.
  # This may not really be necessary, as creating a new entity map with a different
  # aggregator largely accomplishes the same thing.
  #
  # filter(entity_map, separator_func) :: EntityMap
  
  # Gets the value for the index key and updates it, all in one pass
  #
  # get_and_update(entity_map, index_key, fun)

  # Gets the value for the index key and updates it. Raises if there is no key.
  #
  # get_and_update!(entity_map, index_key, fun)  

  # Merges two EntityMaps into one. All entity keys in map2 will be added to map1. 
  # If an entity key already exists in map1, the attribute values of the entity
  # value in map2 will overwrite those in the value of the entity in map1.
  #
  # merge(entity_map1, entity_map2)
  
  # Create an entity map from another entity map.
  #
  # new(entity_map, [entity_map_option]) :: EntityMap.t

  # Partitions an EntityMap into two EntityMaps according to the partition function,
  # which takes a key/aggregate pair and returns true or false.
  # Optional aggregators may be passed in to become the new aggregators for each map.
  # This may not really be necessary, as creating multiple entity maps with the
  # same underlying data but different aggregators largely accomplishes the same thing. 
  # 
  # partition(entity_map, separator_func) :: {EntityMap, EntityMap}
  
  # Takes all entries corresponding to the given entity keys and extracts them 
  # into a separate EntityMap. Returns a tuple with the new map and the old map 
  # with removed keys. Keys for which there are no entires in the map are ignored.
  #
  # split(entity_map, entity_keys)
  
  # Takes all entries corresponding to the given entity keys and returns them in 
  # a new EntityMap.
  #
  # take(entity_map, entity_keys)
end
