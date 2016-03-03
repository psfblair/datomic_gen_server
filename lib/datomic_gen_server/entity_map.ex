defmodule DatomicGenServer.EntityMap do
  # SEMANTICS
  # You can create or update using DataTuples or records/rows.
  # If you have a DataTuple and you provide a nil value for an attribute, 
  #     if the attribute is cardinality/one the nil value deletes the attribute (makes it nil)
  #     if the attribute is cardinality/many the updated value is the empty set.
  # This takes place whether or not other values are provided for the same attribute (in the same set of tuples)
  # If you have a record, if there is no value provided for an attribute, then it is
  #    the equivalent of a nil for a cardinality one attribute or an empty set for a cardinality many attribute.
  # If the values of an entity's attributes are only nil or the empty set, it is removed from the map.

  # If a value to add is provided for a cardinality many attribute in a DataTuple
  # If it is a scalar value it is added to the set.
  # If it is a nonempty collection (set or list) it is unioned with the set.
  # If it is nil or an empty collection the value is reset to the empty set.
  
  # If a value is provided for a cardinality many attribute in a record, it always overwrites.
      
  # If you provide a DataTuple which retracts a nil or empty set value, it retracts 
  # the existing value. This is part of the mechanism that allows us to overwrite
  # an existing cardinality many value with a new value when we are using records.

  # We use an empty tuple as a null marker. Otherwise, we cannot distinguish
  # between a key not being in the map when we are adding new values, and a value
  # which is being set to null when a group of data tuples is being passed in.
  # This makes sure the null carries through if 
  # many values are being added for a given attribute, only one of which is null.
  
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
      aggregate_field_to_raw_attribute: invert_key_rename_map(options[:aggregate_into])
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
  
  # TODO Allow multiple aggregators
  # TODO if struct fails the given attribute map is removed - the entity doesn't wind up in the aggregated map
  # Document - you can filter maps using aggregators since anything failing the struct isn't included.
  # Document - you can take the same raw data and create multiple entities with different aggregators to partition the result set.
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
  
  defp invert_key_rename_map(aggregator_pair) do
    case aggregator_pair do
      {_, key_rename_map} ->
        key_rename_map
        |> Enum.map(fn({k,v}) -> {v, k} end)
        |> Enum.into(%{})
      _ -> nil
    end
  end

  # The aggregator function is used to convert DataTuples to structs of your choosing.
  # If no conversion function is specified, the value for an entity key will be
  # a map from attribute to value. Note that if you want default values for your
  # struct to be used when the map doesn't contain a value for that field, the
  # aggregator should rename the underlying map keys using rename_keys and then
  # call struct on it.
  # The aggregator is stored with the entity map; it is assumed that all the DataTuples
  # or records that you will be adding to or removing from this entity map have the
  # same relevant attributes and so will be aggregated the same way. If you need to
  # change the aggregator on an entity map...
  # Note that there may be entities of different types in the collection of entities
  # passed to the function. This means the aggregator may need to create different
  # structs depending on the attributes in the attribute map. 
  # If an entity map contains entities of different types, the map can be separated
  # according to type using `filter` or `partition`. This may not be necessary
  # unless you want to index the map by an attribute that is only present in some 
  # of the entity types. If you don't care about the entities without the attribute
  # you are indexing on, you can still index by that attribute and the entities
  # without that attribute will go into the map with the entity key `nil`.
  # On "new," DataTuples that are not added are ignored. 
  # The inner map by default contains a field :"datom/e" for the entity ID.
  # Cardinality many is the name of the _incoming_ attribute with cardinality many
  # Index by is the name of the _aggregated_ attribute to index by.
  # Note - if a cardinality many attribute isn't in the DataTuples, it is not in the
  # map either. Trying to get its value won't give you an empty set; it will give
  # you null.
  @spec new([DataTuple.t], [entity_map_option]) :: EntityMap.t
  def new(data_tuples_to_add \\ [], options \\ []) do
    opts = set_defaults(options)

    raw_data_map = 
      data_tuples_to_add
      |> Enum.filter(fn(data_tuple) -> data_tuple.added end)
      |> fold_into_record_map_with_null_markers(opts[:cardinality_many])
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
  
  # We use the empty tuple as a marker for any value to be nullified.
  # If there is a pre-existing value for an attribute, and it is the empty tuple,
  # then that value remains the empty tuple, regardless of the incoming value.
  # If there is no value for an attribute, and the incoming value is nil, an
  # empty set, or an empty list, then that value is set to the empty tuple.
  # (DataTuples, unlike Datoms, may have collections as values for attributes.)
  @spec fold_into_record_map_with_null_markers([DataTuple.t], MapSet.t) :: map
  defp fold_into_record_map_with_null_markers(data_tuples, cardinality_set) do
    List.foldl(data_tuples, %{}, 
      fn(data_tuple, accumulator) -> 
        updated_record = 
          if existing_record = Map.get(accumulator, data_tuple.e) do
            add_attribute_value(existing_record, data_tuple.a, data_tuple.v, cardinality_set)
          else
            new_record_for(data_tuple.e, data_tuple.a, data_tuple.v, cardinality_set)
          end
        Map.put(accumulator, data_tuple.e, updated_record)
      end)    
  end

  @spec new_record_for(term, term, term, MapSet.t) :: map
  defp new_record_for(entity_id, attr, value, cardinality_many) do
    new_record = add_attribute_value(%{}, attr, value, cardinality_many)
    add_attribute_value(new_record, e_key, entity_id, cardinality_many) 
  end

  # If we have multiple DataTuples, each with a scalar value for a cardinality many
  # attribute, we add each new value to the set of prior values. In a DataTuple,
  # unlike with a simple datom, we might get a collection as a value. In that 
  # case, since we are constructing a new EntityMap here, we shouldn't get more
  # than one DataTuple for that attribute of that entity. If by chance we do, 
  # we union it with the previous value.
  @spec add_attribute_value(map, term, term, MapSet.t) :: map
  defp add_attribute_value(attr_map, attr, value, cardinality_many) do

    prior_value = initialize_if_needed(attr, Map.get(attr_map, attr), cardinality_many)
    value_with_null_marker = with_null_marker(value)    
    
    # TODO This duplicates code on line 421
    new_value = case {prior_value, value_with_null_marker} do
      {_, {}} -> {}
      {{}, _} -> {}
      {%MapSet{}, [_|_]} -> MapSet.union(prior_value, MapSet.new(value_with_null_marker))
      {%MapSet{}, %MapSet{}} -> MapSet.union(prior_value, value_with_null_marker)
      {%MapSet{}, _} -> MapSet.put(prior_value, value_with_null_marker)
      _ -> value_with_null_marker
    end
    
    # We can't filter out the null markers until we get the final attribute map.
    Map.put(attr_map, attr, new_value)  
  end
  
  @spec initialize_if_needed(term, term, MapSet.t) :: term
  defp initialize_if_needed(attr, prior_value, cardinality_many) do
    if prior_value do
      prior_value
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
  
  @spec filter_null_attributes(map, MapSet.t) :: map
  defp filter_null_attributes(entity_id_to_attributes, cardinality_many) do 
    null_cardinality_many_attribute_to_empty_set = 
      fn({attr, val}) -> 
        case val do 
          {} -> if MapSet.member?(cardinality_many, attr) do {attr, MapSet.new()} else {attr, val} end
          _ -> {attr, val}
        end
      end
    
    absent_cardinality_many_attribute_to_empty_set =
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
        |> absent_cardinality_many_attribute_to_empty_set.()
        |> Enum.map((&null_cardinality_many_attribute_to_empty_set.(&1)))
        |> Enum.filter((&is_non_null_attribute.(&1)))
        |> Enum.into(%{})
      end

    entity_id_to_attributes |> Enum.map(
      fn({entity_id, attr_map}) -> 
        {entity_id, handle_null_attributes.(attr_map)} 
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
    |> Enum.filter(fn({attr, value}) ->     
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
  
  # A record is a map of attribute names to values. One of those attribute keys must
  # point to the entity primary key -- i.e., the same value that would appear
  # in the `e` value of a corresponding datom or DataTuple.
  # If two records share the same identifier, records are merged. For cardinality/one
  # attributes this means that one value overwrites the other (this is non-deterministic).
  # For any cardinality/many attributes values that are not already sets are
  # turned into sets and values that are merged in are added to those sets.
  # If your record already has a list or set as a value for an attribute, then if
  # it is listed as a cardinality many attribute, other values that are lists or sets will
  # be merged together. If it is not listed as a cardinality many attribute, then
  # successive values of a collection will overwrite the previous ones.
  # NOTE - INDEXING HAPPENS ON THE ATTRIBUTE IN THE STRUCT, NOT IN THE INCOMING
  # ATTRIBUTES. CARDINALITY MANY IS A NAME OF AN INCOMING ATTRIBUTE  
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

  # A row is a simple list of attribute values. The header parameter supplies
  # the attribute names for these values. One of those attribute keys must
  # point to the entity primary key -- i.e., the same value that would appear
  # in the `e` value of a corresponding datom or DataTuple.
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
      |> fold_into_record_map_with_null_markers(entity_map.cardinality_many)
      |> Enum.reduce(entity_map.raw_data, &retract/2)
      |> filter_null_attributes(entity_map.cardinality_many)
      |> filter_null_entities

    raw_data_map_with_all_changes =
      data_tuples_to_add
      |> fold_into_record_map_with_null_markers(entity_map.cardinality_many)
      |> Enum.reduce(raw_data_map_with_retractions, &add/2)
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
  
  @spec add({term, map}, map) :: map
  defp add({entity_id, attributes_to_add}, entity_map) do
    
    existing_attributes = Map.get(entity_map, entity_id) || %{}
    
    # If the value to add is nil, it has already been changed to the null marker {}.
    # TODO This duplicates code on line 183
    merge_fn = fn(_, old_value, new_value) ->      
      case {old_value, new_value} do
        {{}, _} -> {}
        {_, {}} -> {}
        {%MapSet{}, %MapSet{}} -> MapSet.union(old_value, new_value)
        {%MapSet{}, [_|_]} -> MapSet.union(old_value, MapSet.new(new_value))
        {%MapSet{}, _} -> MapSet.put(old_value, new_value)    
        _ -> new_value
      end
    end
    
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
  
  # TODO
  # aggregate_by(entity_map, aggregate)
  
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
  # drop(entity_map, entity_keys)

  # Checks if two EntityMaps contain equal data. Their index_by and aggregator
  # are ignored.
  @spec equal?(EntityMap.t, EntityMap.t) :: boolean
  def equal?(entity_map1, entity_map2) do
    Map.equal?(entity_map1.inner_map, entity_map2.inner_map)
  end

  # TODO Fetches the value for a specific entity key and returns it in a tuple
  # If the key does not exist, returns :error.
  # fetch(entity_map, entity_key)
  
  # TODO 
  # fetch_attr(entity_map, entity_key, attr_key)
  
  # TODO Fetches the value for a specific entity key.
  # If the key does not exist, a `KeyError` is raised.
  # fetch!(entity_map, entity_key)
  
  # TODO 
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
      
    %__MODULE__{inner_map: new_inner, 
                index_by: attribute, 
                cardinality_many: entity_map.cardinality_many,
                aggregator: entity_map.aggregator,
                aggregate_field_to_raw_attribute: entity_map.aggregate_field_to_raw_attribute
               }
  end
  
  # Removes nil keys
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
      raw_data_index_attr = aggregate_field_to_raw_attribute(entity_map, entity_map.index_by)      
      
      {entity_id, _} = 
        Enum.find(entity_map.raw_data, fn({entity_id, attributes}) -> 
          Map.get(attributes, raw_data_index_attr) == index_key 
        end)
        
      entity_id
    end
  end
  
  @spec aggregate_field_to_raw_attribute(EntityMap.t, term) :: term
  defp aggregate_field_to_raw_attribute(entity_map, attr_key) do
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
    raw_attr_key = aggregate_field_to_raw_attribute(entity_map, attr_key)
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
  
  
  # TODO Additional possibilities for the future:

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
