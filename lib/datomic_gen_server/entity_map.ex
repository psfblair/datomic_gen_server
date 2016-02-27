defmodule DatomicGenServer.EntityMap do
  
  defstruct raw_data: %{}, inner_map: %{}, index_by: nil, cardinality_many: [], aggregator: &__MODULE__.identity/1
  @type t :: %DatomicGenServer.EntityMap{raw_data: map, inner_map: map, index_by: term, aggregator: (Datom.t -> term)}
  
  defmodule DataTuple do
    defstruct e: nil, a: 0, v: nil, added: false
    @type t :: %DataTuple{e: term, a: atom, v: term, added: boolean}
  end
  
  @spec e_key :: :"datom/e"
  def e_key, do: :"datom/e"
  
  @spec identity(term) :: term
  def identity(x), do: x
  
  @type aggregator :: (Datom.t -> term)
  @type entity_map_option :: {atom, term} | {atom, MapSet.t} | {atom, aggregator}
  
  @spec set_defaults([entity_map_option]) :: [entity_map_option]
  def set_defaults(options) do
    [ index_by: options[:index_by],
      cardinality_many: to_set(options[:cardinality_many]),
      aggregator: options[:aggregator] || &identity/1
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

  # TODO Create an entity map from another entity map.

  # The aggregator function is used to convert Datoms to structs of your choosing.
  # If no conversion function is specified, the value for an entity key will be
  # a map from attribute to value. Note that if you want default values for your
  # struct to be used when the map doesn't contain a value for that field, the
  # aggregator should rename the underlying map keys using rename_keys and then
  # call struct on it.
  # The aggregator is stored with the entity map; it is assumed that all the datoms
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
  # On "new," Datoms that are not added are ignored. 
  # The inner map by default contains a field :"datom/e" for the entity ID.
  # Cardinality many is the name of the _incoming_ attribute with cardinality many
  # Index by is the name of the _aggregated_ attribute to index by.
  @spec new([Datom.t], [entity_map_option]) :: EntityMap.t
  def new(datoms_to_add \\ [], options \\ []) do
    opts = set_defaults(options)

    raw_data_map = 
      datoms_to_add
      |> Enum.filter(fn(datom) -> datom.added end)
      |> fold_into_record_map(opts[:cardinality_many])
      
    inner_map =
      raw_data_map
      |> to_aggregated_map(opts[:aggregator])
      |> index_if_necessary(opts[:index_by])
    
    entity_map = 
      %__MODULE__{raw_data: raw_data_map,
                  inner_map: inner_map, 
                  index_by: opts[:index_by], 
                  cardinality_many: opts[:cardinality_many], 
                  aggregator: opts[:aggregator]}
  end
  
  @spec to_aggregated_map(map, aggregator) :: map
  defp to_aggregated_map(entities_by_id, aggregator) do
    entities_by_id
    |> Enum.map(fn({entity_id, attr_map}) -> {entity_id, aggregator.(attr_map)} end)
    |> Enum.into(%{})  
  end
  
  @spec fold_into_record_map([DataTuple.t], MapSet.t) :: map
  defp fold_into_record_map(datoms, cardinality_set) do
    List.foldl(datoms, %{}, 
      fn(datom, accumulator) -> 
        updated_record = 
          if existing_record = Map.get(accumulator, datom.e) do
            add_attribute_value(existing_record, datom.a, datom.v, cardinality_set)
          else
            new_record_for(datom.e, datom.a, datom.v, cardinality_set)
          end
        Map.put(accumulator, datom.e, updated_record)
      end)
  end
  
  @spec add_attribute_value(map, term, term, MapSet.t) :: map
  defp add_attribute_value(attr_map, attr, value, cardinality_many) do
    if MapSet.member?(cardinality_many, attr) do
      prior_values = Map.get(attr_map, attr)
      new_value = merge_value_with_set(prior_values, value)
      Map.put(attr_map, attr, new_value)
    else
      Map.put(attr_map, attr, value)
    end
  end
  
  @spec add_value_to_set(MapSet.t, MapSet.t | list | term ) :: MapSet.t
  defp merge_value_with_set(set, value) do
    if set do 
      add_value_to_set(set, value)
    else
      new_set_from_value(value)
    end    
  end
  
  @spec add_value_to_set(MapSet.t, MapSet.t | list | term ) :: MapSet.t
  defp add_value_to_set(set, value) do
    case value do
      %MapSet{} -> MapSet.union(set, value) 
      [h|t] -> MapSet.union(set, MapSet.new(value))
      [] -> set
      _ -> MapSet.put(set, value)
    end
  end
  
  @spec new_set_from_value(term) :: MapSet.t
  defp new_set_from_value(value) do
    case value do
      %MapSet{} -> value 
      [h|t] -> MapSet.new(value)
      [] -> MapSet.new()
      _ -> MapSet.new([value])
    end    
  end

  @spec new_record_for(term, term, term, MapSet.t) :: map
  defp new_record_for(entity_id, attr, value, cardinality_many) do
    new_record = add_attribute_value(%{}, attr, value, cardinality_many)
    add_attribute_value(new_record, e_key, entity_id, cardinality_many) 
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
  # in the `e` value of a corresponding datom.
  # If two records share the same identifier, records are merged. For cardinality/one
  # attributes this means that one value overwrites the other (this is non-deterministic).
  # For any cardinality/many attributes values that are not already sets are
  # turned into sets and values that are merged in are added to those sets.
  # If your record already has a list or set as a value for an attribute, then if
  # it is listed as a cardinality many attribute, other values that are lists or sets will
  # be merged together. If it is not listed as a cardinality many attribute, then
  # successive values of a collection will overwrite the previous ones.
  @spec from_records([map] | MapSet.t, term, [entity_map_option]) :: EntityMap.t
  def from_records(record_maps_to_add, primary_key, options \\ []) do
    opts = set_defaults(options)

    raw_data_map =
      record_maps_to_add 
      |> Enum.map(fn(attr_map) -> 
          entity_id = Map.get(attr_map, primary_key)
          Enum.map(attr_map, fn{attr, value} -> 
             %DataTuple{e: entity_id, a: attr, v: value, added: true}
           end)
         end)
      |> Enum.concat
      |> fold_into_record_map(opts[:cardinality_many])
      
    inner_map =
      raw_data_map
      |> to_aggregated_map(opts[:aggregator])
      |> index_if_necessary(opts[:index_by])
      
    entity_map = %__MODULE__{raw_data: raw_data_map,
                             inner_map: inner_map, 
                             index_by: opts[:index_by], 
                             cardinality_many: opts[:cardinality_many], 
                             aggregator: opts[:aggregator]}
  end

  # A row is a simple list of attribute values. The header parameter supplies
  # the attribute names for these values. One of those attribute keys must
  # point to the entity primary key -- i.e., the same value that would appear
  # in the `e` value of a corresponding datom.
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
  
  @spec update(EntityMap.t, [Datom.t]) :: EntityMap.t
  def update(entity_map, datoms_to_update) do
    {datoms_to_add, datoms_to_retract} = 
      Enum.partition(datoms_to_update, fn(datom) -> datom.added end)
    
    raw_data_map_with_retractions =
      datoms_to_retract
      |> fold_into_record_map(entity_map.cardinality_many)
      |> Enum.reduce(entity_map.raw_data, &retract/2)
      
    raw_data_map_with_all_changes =
      datoms_to_add
      |> fold_into_record_map(entity_map.cardinality_many)
      |> Enum.reduce(raw_data_map_with_retractions, &add/2)

    updated_inner_map =
      raw_data_map_with_all_changes
      |> to_aggregated_map(entity_map.aggregator)
      |> index_if_necessary(entity_map.index_by)
    
    entity_map = %__MODULE__{raw_data: raw_data_map_with_all_changes,
                             inner_map: updated_inner_map, 
                             index_by: entity_map.index_by, 
                             cardinality_many: entity_map.cardinality_many, 
                             aggregator: entity_map.aggregator}
  end
  
  @spec add({term, map}, map) :: map
  defp add({entity_id, attributes_to_add}, entity_map) do
    existing_attributes = Map.get(entity_map, entity_id) || %{}
    new_attr_map = Map.merge(existing_attributes, attributes_to_add)
    Map.put(entity_map, entity_id, new_attr_map)
  end
  
  @spec retract({term, map}, map) :: map
  defp retract({entity_id, attributes_to_retract}, entity_map) do
    existing_attributes = Map.get(entity_map, entity_id) || %{}
    
    new_attr_map = 
      attributes_to_retract
      |> Map.delete(:__struct__) 
      |> Enum.reduce(existing_attributes, &remove_attribute_value/2)

    if is_empty_entity(new_attr_map) do
      Map.delete(entity_map, entity_id)
    else
      Map.put(entity_map, entity_id, new_attr_map)
    end
  end
  
  # When retracting from an attribute with cardinality many, the value has
  # to be removed from the Set of values if it's one value, or the set of values
  # subtracted if it's a set of values.
  # You can pass in a nil value or an empty collection to remove a key from 
  # the map (if it's not a struct) or to turn the value to nil/empty if it is a
  # struct. Datomic datoms won't have these values so it's ok.
  defp remove_attribute_value({attr, value}, attr_map_or_struct) do
    if Map.get(attr_map_or_struct, :"__struct__") do
      # TODO TEST
      remove_attribute_value_from_struct(attr_map_or_struct, attr, value)
    else
      # TODO TEST
      remove_attribute_value_from_map(attr_map_or_struct, attr, value)
    end
  end
  
  # Will only retract the value if the struct contains that value for that attribute.
  # For structs, regardless of the current value of the attribute, you can also
  # pass in a nil value or an empty collection to to turn the attribute value to 
  # nil/empty. Datomic datoms won't ever come back with nil or [] values so it's
  # ok to use these in a struct to signify empty values, since there's never nils
  # in the database. (This is mainly for use with records.)
  # When retracting from an attribute with cardinality many, the value is
  # removed from the set of values if it's one value, or the set of values
  # subtracted if it's a set of values.
  defp remove_attribute_value_from_struct(attr_struct, attr, value) do
    old_value = Map.get(attr_struct, attr) 
    empty_set = MapSet.new()
    set_of_nil = MapSet.new([nil])

    case {old_value, value} do
      {_, nil} -> 
        Map.put(attr_struct, attr, nil)
      {_, []} -> 
        Map.put(attr_struct, attr, nil)
      {_, %MapSet{}} when value == empty_set or value == set_of_nil ->
        Map.put(attr_struct, attr, nil)
      {old, val} when val == old -> 
        Map.put(attr_struct, attr, nil)
      {%MapSet{}, %MapSet{}} ->
        Map.put(attr_struct, attr, MapSet.difference(old_value, value))
      {%MapSet{}, [h|t]} -> 
        Map.put(attr_struct, attr, MapSet.difference(old_value, MapSet.new(value)))
      {%MapSet{}, _} -> 
        Map.put(attr_struct, attr, MapSet.delete(old_value, value))
      _ ->
        attr_struct
    end
  end 
    
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
  defp remove_attribute_value_from_map(attr_map, attr, value) do
    attribute_is_datom_e = attr == e_key
    old_value = Map.get(attr_map, attr)
    empty_set = MapSet.new()
    set_of_nil = MapSet.new([nil])

    case {old_value, value} do
      {_, _} when attribute_is_datom_e ->
        attr_map    
      {_, nil} -> 
        Map.delete(attr_map, attr)
      {_, []} -> 
        Map.delete(attr_map, attr)
      {_, %MapSet{}} when value == empty_set or value == set_of_nil ->
        Map.delete(attr_map, attr)
      {old, val} when val == old -> 
        Map.delete(attr_map, attr)
      {%MapSet{}, %MapSet{}} ->
        Map.put(attr_map, attr, MapSet.difference(old_value, value))
      {%MapSet{}, [h|t]} -> 
        Map.put(attr_map, attr, MapSet.difference(old_value, MapSet.new(value)))
      {%MapSet{}, _} -> 
        Map.put(attr_map, attr, MapSet.delete(old_value, value))
      _ ->
        attr_map
    end
  end 
  
  # TODO Test
  # If all the attribute keys have been removed from the map, or if all the fields
  # on a struct are nil or empty, then it is "empty" and will be removed from the entity map.
  # If only the e_key is left, it's empty.
  @spec is_empty_entity(map) :: boolean
  defp is_empty_entity(attr_map_or_struct) do
    empty_set = MapSet.new()
    attr_map_or_struct 
    |> Map.delete(:"__struct__")
    |> Map.delete(e_key)          
    |> Enum.all?(fn({k,v}) -> 
        case v do
          nil -> true
          %MapSet{} when v == empty_set -> true
          [] -> true
          _ -> false
        end
       end)
  end

  # A record is a map of attribute names to values. One of those attribute keys 
  # must uniquely identify the entity to which the attributes pertain. This 
  # attribute should be passed to the function as the third argument.
  # The records will be converted to datoms with the e: field being the value
  # of the entity identifier.
  # The update function taking two datom lists will then be called with the result.
  @spec update_r(EntityMap.t, Enum.t, term) :: EntityMap.t
  def update_r(entity_map, records, entity_identifier) do
    %__MODULE__{}
  end
  
  @spec update_t(EntityMap.t, DatomicTransaction.t) :: EntityMap.t
  def update_t(entity_map, {datoms_to_retract, datoms_to_add}) do
    %__MODULE__{}
  end
  
  # Creates a new EntityMap whose keys are values of a certain attribute or 
  # struct field rather than the entity IDs.
  # Assumes that the value you are indexing on is unique, otherwise you will lose
  # entities. If a value for the attribute is not present in the attribute map/struct,
  # it will go into the entity map with a key of nil.
  # index_by is applied after your aggregator, so you can use the name of a field
  # in your struct. If a field in the datoms isn't in the struct, you can't index by it.
  @spec index_by(EntityMap.t, term) :: EntityMap.t
  def index_by(entity_map, attribute) do
    new_inner = entity_map.inner_map |> index_map_by(attribute)
      
    %__MODULE__{inner_map: new_inner, 
                index_by: attribute, 
                cardinality_many: entity_map.cardinality_many,
                aggregator: entity_map.aggregator}
  end
  
  @spec index_map_by(map, term) :: map
  defp index_map_by(entity_mapping, attribute) do
    entity_mapping
    |> Enum.map(fn({_, attributes}) -> {Map.get(attributes, attribute), attributes} end)
    |> Enum.into(%{})
  end
  
  # Pass in a map and another map from keys to new keys.
  @spec rename_keys(map, map) :: EntityMap.t
  def rename_keys(map, key_rename_map) do
    Enum.map(map, fn({k,v}) ->             
      new_name = Map.get(key_rename_map, k)
      if new_name do {new_name, v} else {k,v} end
    end)                                       
    |> Enum.into(%{})                          
  end
  
  # Map functions
  
  # Deletes the entry in the EntityMap for a specific entity key. 
  # If the key does not exist, returns the map unchanged.
  # delete(entity_map, entity_key)

  # Deletes the entries for all of the given keys from the map.
  # drop(entity_map, entity_keys)

  # Checks if two EntityMaps contain equal data. Their index_by and aggregator
  # are ignored.
  @spec equal?(EntityMap.t, EntityMap.t) :: boolean
  def equal?(entity_map1, entity_map2) do
    Map.equal?(entity_map1.inner_map, entity_map2.inner_map)
  end

  # Fetches the value for a specific entity key and returns it in a tuple
  # If the key does not exist, returns :error.
  # fetch(entity_map, entity_key)
  
  # fetch_attr(entity_map, entity_key, attr_key)
  
  # Fetches the value for a specific entity key.
  # If the key does not exist, a `KeyError` is raised.
  # fetch!(entity_map, entity_key)
  
  # fetch_attr!(entity_map, entity_key, attr_key)
  
  # Filters out only certain entity key/attribute map pairs according to the function,
  # which takes a pair and returns true or false
  # An optional aggregator may be passed in to become the aggregator for the new map.
  # However, it is not used in the actual filtering.
  # filter(entity_map, separator_func) :: EntityMap

  # Gets the value for a specific entity key.
  # If key does not exist, returns the default value (nil if no default value).
  # get(entity_map, entity_key, default \\ nil)
  
  # get_attr(entity_map, entity_key, attr_key, default \\ nil)
  
  # Returns whether a given entity key exists in the given map
  # has_key?(entity_map, entity_key)
  
  # Selects a group of attributes and populates an entity map containing only those
  # into(entity_map, attrs)
  # into(entity_map, attrs, aggregator)
  
  # Returns all entity keys from the map
  # keys(entity_map)
  
  # Merges two maps into one. All entity keys in map2 will be added to map1. 
  # If an entity key already exists in map1, the attribute values of the entity
  # value in map2 will overwrite those in the value of the entity in map1.
  # merge(entity_map1, entity_map2)
  
  # Partitions an EntityMap into two EntityMaps according to the function,
  # which takes a pair and returns true or false
  # Optional aggregators may be passed in to become the new aggregators for each map.
  # However, they are not used in the partitioning.
  # partition(entity_map, separator_func) :: {EntityMap, EntityMap}

  # Returns and removes the value associated with an entity key in the map
  # pop(entity_map, entity_key, default \\ nil)
  
  # Puts the given value under the entity key
  # put(entity_map, entity_key, val)
  
  # put_attr(entity_map, entity_key, attr_key, val)
  
  # Puts the given value under key unless the entry key already exists
  # put_new(entity_map, entity_key, value)
  
  # put_attr_new(entity_map, entity_key, attr_key, val)
  
  # Takes all entries corresponding to the given entity keys and extracts them 
  # into a separate EntityMap. Returns a tuple with the new map and the old map 
  # with removed keys. Keys for which there are no entires in the map are ignored.
  # split(entity_map, entity_keys)
  
  # Takes all entries corresponding to the given entity keys and returns them in 
  # a new EntityMap.
  # take(entity_map, entity_keys)
  
  # Converts the EntityMap to a list
  # to_list(entity_map)
  
  # Returns all values from the EntityMap
  # values(entity_map)

  
  
  # TODO - Map functions to support some time in the future
  # get_and_update(entity_map, entity_key, fun)
  #      Gets the value from key and updates it, all in one pass
  # Get_and_update!(entity_map, entity_key, fun)  
  #      Gets the value from key and updates it. Raises if there is no key
  # get_lazy(entity_map, key, fun)
  #      Gets the value for a specific key. If key does not exist, lazily evaluates
  #      fun and returns its result.
  # pop_lazy(entity_map, key, fun)  
  #      Lazily returns and removes all values associated with key in the map
  # put_new_lazy(entity_map, key, fun)
  #      Evaluates fun and puts the result under key in map unless key is already present

end
