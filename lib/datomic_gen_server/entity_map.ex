defmodule DatomicGenServer.EntityMap do
  
  defstruct inner_map: %{}, index_by: nil, cardinality_many: [], aggregator: &__MODULE__.identity/1
  @type t :: %DatomicGenServer.EntityMap{inner_map: map, index_by: term, aggregator: (Datom.t -> term)}
  
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

  # TODO Test with various different sorts of entity in the same map
  # TODO filter function for separating an EntityMap into multiple maps, by entity.

  # The aggregator function is used to convert Datoms to structs of your choosing.
  # If no conversion function is specified, the value for an entity key will be
  # a map from attribute to value. Note that if you want default values for your
  # struct to be used when the map doesn't contain a value for that field, the
  # aggregator should rename the underlying map keys using rename_keys and then
  # call struct on it.
  # Note that there may be entities of different types in the collection of entities
  # passed to the function. This means the aggregator may need to create different
  # structs depending on the attributes in the attribute map. 
  # If an entity map contains entities of different types, the map can be separated
  # according to type using. This may not be necessary unless you are indexing 
  # the map by an attribute that is only present in some of the entity types.
  # On "new," Datoms that are not added are ignored. 
  # The inner map by default contains a field :"datom/e" for the entity ID.
  @spec new([Datom.t], [entity_map_option]) :: EntityMap.t
  def new(datoms_to_add \\ [], options \\ []) do
    opts = set_defaults(options)

    inner_map = 
      datoms_to_add
      |> Enum.filter(fn(datom) -> datom.added end)      
      |> fold_into_record_map(opts[:cardinality_many], true)
      |> Enum.map(fn({entity_id, attr_map}) -> {entity_id, opts[:aggregator].(attr_map)} end)
      |> Enum.into(%{})
    
    entity_map = 
      %__MODULE__{inner_map: inner_map, 
                  index_by: opts[:index_by], 
                  cardinality_many: opts[:cardinality_many], 
                  aggregator: opts[:aggregator]}

    if opts[:index_by] do index_by(entity_map, opts[:index_by]) else entity_map end
  end
  
  @spec fold_into_record_map([DataTuple.t], MapSet.t, boolean) :: map
  defp fold_into_record_map(datoms, cardinality_set, include_entity_id?) do
    datoms
    |> List.foldl(%{}, fn(datom, acc) -> 
          entity_id = datom.e
          entity_record = Map.get(acc, entity_id)
          updated_record = 
            if entity_record do
              add_attribute_value(entity_record, datom.a, datom.v, cardinality_set)
            else
              new_record = add_attribute_value(%{}, datom.a, datom.v, cardinality_set)
              if include_entity_id? do 
                add_attribute_value(new_record, e_key, entity_id, cardinality_set) 
              else 
                new_record 
              end
            end
          Map.put(acc, entity_id, updated_record)
       end)
  end
  
  defp add_attribute_value(attr_map, attr, value, cardinality_many) do
    if MapSet.member?(cardinality_many, attr) do
      prior_values = Map.get(attr_map, attr)
      new_value = merge_value_with_set(prior_values, value)
      Map.put(attr_map, attr, new_value)
    else
      Map.put(attr_map, attr, value)
    end
  end
  
  defp remove_attribute_value(attr_map, attr, value) do
    old_value = Map.get(attr_map, attr)
    if ! Map.get(attr_map, :"__struct__") && value == old_value do
      Map.delete(attr_map, attr)
    else
      new_value = case old_value do
        %MapSet{} -> MapSet.delete(value)
        val when val == value -> nil
        val -> val
      end
      Map.put(attr_map, attr, new_value)
    end
  end
  
  defp merge_value_with_set(set, value) do
    if set do 
      add_value_to_set(set, value)
    else
      new_set_from_value(value)
    end    
  end
  
  defp add_value_to_set(set, value) do
    case value do
      %MapSet{} -> MapSet.union(set, value) 
      [h|t] -> MapSet.union(set, MapSet.new(value))
      [] -> set
      _ -> MapSet.put(set, value)
    end
  end
  
  defp new_set_from_value(value) do
    case value do
      %MapSet{} -> value 
      [h|t] -> MapSet.new(value)
      [] -> MapSet.new()
      _ -> MapSet.new([value])
    end    
  end
  
  # A record is a map of attribute names to values. One of those attribute keys 
  # must uniquely identify the entity to which the attributes pertain. This must
  # be passed as an :index_by option
  # If two records share the same identifier, records are merged. For cardinality/one
  # attributes this means that one value overwrites the other (this is non-deterministic).
  # For any cardinality/many attributes values that are not already sets are
  # turned into sets and values that are merged in are added to those sets.
  # If your record already has a list or set as a value for an attribute, then if
  # it is listed as a cardinality many attribute, other values that are lists or sets will
  # be merged together. If it is not listed as a cardinality many attribute, then
  # successive values of a collection will overwrite the previous ones.
  @spec from_records([map] | MapSet.t, [entity_map_option]) :: EntityMap.t
  def from_records(record_maps_to_add, options \\ []) do
    unless options[:index_by] do raise(":index_by option required for new_r") end
    opts = set_defaults(options)

    inner_map = 
      record_maps_to_add 
      |> Enum.map(fn(attr_map) -> 
           Enum.map(attr_map, fn{attr, value} -> 
             %DataTuple{e: attr_map[opts[:index_by]], a: attr, v: value, added: true}
           end)
         end)
      |> Enum.concat
      |> fold_into_record_map(opts[:cardinality_many], false)
      |> Enum.map(fn({entity_id, attr_map}) -> {entity_id, opts[:aggregator].(attr_map)} end) 
      |> Enum.into(%{})
    
    entity_map = %__MODULE__{inner_map: inner_map, 
                             index_by: opts[:index_by], 
                             cardinality_many: opts[:cardinality_many], 
                             aggregator: opts[:aggregator]}
  end

  # A row is a simple list of attribute values. The header parameter supplies
  # the attribute names for these values. One of those attribute keys 
  # must uniquely identify the entity to which the attributes pertain. This must
  # be passed as an :index_by option
  @spec from_rows([list] | MapSet.t, list, [entity_map_option]) :: EntityMap.t
  def from_rows(rows_to_add, header, options \\ []) do
    rows_to_add
    |> rows_to_records(header)
    |> from_records(options)    
  end
  
  @spec rows_to_records([list] | MapSet.t, list) :: [map]
  defp rows_to_records(rows, header) do
    Enum.map(rows, fn(row) -> Enum.zip(header, row) |> Enum.into(%{})  end)
  end
  
  @spec from_transaction(DatomicTransaction.t, [entity_map_option]) :: EntityMap.t
  def from_transaction(transaction, options \\ []) do
    new(transaction.added_datoms, options)
  end
  
  @spec update(EntityMap.t, [Datom.t], [Datom.t]) :: EntityMap.t
  def update(entity_map, datoms_to_retract, datoms_to_add) do
    datoms_retracted_by_entity = fold_into_record_map(datoms_to_retract, entity_map.cardinality_many, entity_map.index_by)
    # TODO retract them - preserve indexing! If it retracts a value that isn't in the map, leave the one in the map.
    
    datoms_added_by_entity_keyword = fold_into_record_map(datoms_to_add, entity_map.cardinality_many, entity_map.index_by)
    # TODO add them - preserve indexing!
    
    %__MODULE__{}
  end
  
  #TODO When retracting from an attribute with cardinality many, the value has
  # to be removed from the Set of values if it's one value, or the set of values
  # subtracted if it's a set of values.
  @spec retract(EntityMap.t, [Datom.t]) :: EntityMap.t
  defp retract(entity_map, retract_datoms_keyword_list) do
    %__MODULE__{}
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
    new_inner = 
      entity_map.inner_map
      |> Enum.map(fn({_, attributes}) -> {Map.get(attributes, attribute), attributes} end)
      |> Enum.into(%{})
      
    %__MODULE__{inner_map: new_inner, 
                index_by: attribute, 
                cardinality_many: entity_map.cardinality_many,
                aggregator: entity_map.aggregator}
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
