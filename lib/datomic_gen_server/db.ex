defmodule DatomicGenServer.Db do
  @moduledoc """
    DatomicGenServer.Db is a module intended to facilitate the use of Elixir
    data structures instead of edn strings for communicating with Datomic. This
    module maps the DatomicGenServer interface functions in wrappers that accept
    and return Elixir data structures, and also provides slightly more syntactically
    pleasant equivalents for Datomic keys and structures that would otherwise
    need to be represented using a lot of punctuation that isn't required in Clojure.
    
    The hexdoc organizes the functions in this module alphabetically; here is a 
    list by topic:
    
## Interface functions    

      q(server_identifier, exdn, options \\ [])
      transact(server_identifier, exdn, options \\ [])
      entity(server_identifier, exdn, attr_names \\ :all, options \\ [])
      seed(server_identifier, migration_path, seed_data_path, options \\ [])

## Datomic Shortcuts
### Id/ident

      dbid(db_part)
      id
      ident

### Transaction creation

      add
      retract
      install_attribute
      alter_attribute
      tx_instant

### Value types

      value_type
      type_long
      type_keyword
      type_string
      type_boolean
      type_bigint
      type_float
      type_double
      type_bigdec
      type_ref
      type_instant
      type_uuid
      type_uri
      type_bytes

### Cardinalities

      cardinality
      cardinality_one
      cardinality_many
  
### Optional Schema Attributes

      doc
      unique
      unique_value
      unique_identity
      index
      fulltext
      is_component
      no_history
  
### Functions

      _fn
      fn_retract_entity
      fn_cas

### Common partions

      schema_partition
      transaction_partition
      user_partition

### Query placeholders

      q?(atom)

### Data sources

      implicit
      inS(placeholder_atom)

### Bindings and find specifications

      single_scalar
      blank_binding
      collection_binding(placeholder_atom)

### Clauses

      _not(inner_clause)
      _not_join(binding_list, inner_clause_list)
      _or(inner_clauses)
      _or_join(binding_list, inner_clause_list)
      _and(inner_clauses)
      _pull({:symbol, entity_var}, pattern_clauses)
      _expr(function_symbol, remaining_expressions, bindings)

## Examples
    
      DatomicGenServer.start(
        "datomic:mem://test", 
        true, 
        [{:timeout, 20_000}, {:default_message_timeout, 20_000}, {:name, DatomicGenServer}]
      )

      data_to_add = [%{ 
          Db.id => Db.dbid(Db.schema_partition),
          Db.ident => :"person/name",
          Db.value_type => Db.type_string,
          Db.cardinality => Db.cardinality_one,
          Db.doc => "A person's name",
          Db.install_attribute => Db.schema_partition
      }]
      Db.transact(DatomicGenServer, data_to_add)
      
      # => {:ok, %DatomicGenServer.Db.DatomicTransaction{
              basis_t_before: 1001,   
              basis_t_after: 1002, 
              retracted_datoms: [],
              added_datoms: [
                %DatomicGenServer.Db.Datom{a: 50, added: true, e: 13194139534314, tx: 13194139534314, 
                    v: %Calendar.DateTime{abbr: "UTC", day: 15, hour: 3, min: 20, month: 2, sec: 1, std_off: 0, 
                                          timezone: "Etc/UTC", usec: 746000, utc_off: 0, year: 2016}},
                %DatomicGenServer.Db.Datom{a: 41, added: true, e: 65, tx: 13194139534314, v: 35},
                %DatomicGenServer.Db.Datom{a: 62, added: true, e: 65, tx: 13194139534314, v: "A person's name"},
                %DatomicGenServer.Db.Datom{a: 10, added: true, e: 65, tx: 13194139534314, v: :"person/name"},
                %DatomicGenServer.Db.Datom{a: 40, added: true, e: 65, tx: 13194139534314, v: 23},
                %DatomicGenServer.Db.Datom{a: 13, added: true, e: 0, tx: 13194139534314, v: 65}],
              tempids: %{-9223367638809264706 => 65}}}

      query = [:find, Db.q?(:c), :where, [Db.q?(:c), Db.doc, "A person's name"]]
      Db.q(DatomicGenServer, query)

      #=> {:ok, #MapSet<['A']>}  # ASCII representation of ID 65
      
  """
  #TODO Use type Exdn.converter in signature instead of (Exdn.exdn -> term)
  @type query_option :: DatomicGenServer.send_option | 
                        {:response_converter, (Exdn.exdn -> term)} | 
                        {:edn_tag_handlers, [{atom, Exdn.handler}, ...]}
  
  @type datom_map :: %{:e => integer, :a => atom, :v => term, :tx => integer, :added => boolean}
  @type transaction_result :: %{:"db-before" => %{:"basis-t" => integer}, 
                                :"db-after" => %{:"basis-t" => integer}, 
                                :"tx-data" => [datom_map], 
                                :tempids => %{integer => integer}}
  
  defmodule DatomicTransaction do
    defstruct basis_t_before: 0, 
              basis_t_after: 0, 
              added_datoms: [], 
              retracted_datoms: [], 
              tempids: %{} 
    @type t :: %DatomicTransaction{basis_t_before: integer, 
                                   basis_t_after: integer, 
                                   added_datoms: [Datom.t], 
                                   retracted_datoms: [Datom.t], 
                                   tempids: %{integer => integer}}
  end
  
  defmodule Datom do
    defstruct a: 0, e: 0, v: [], tx: %{}, added: false
    @type t :: %Datom{e: integer, a: atom, v: term, tx: integer, added: boolean}
  end

############################# INTERFACE FUNCTIONS  ############################
  @doc """
  Queries a DatomicGenServer using a query formulated as an Elixir list.
  This query is translated to an edn string which is then passed to the Datomic 
  `q` API function. 
  
  The first parameter to this function is the pid or alias of the GenServer process; 
  the second is the query. 
  
  The options keyword list may include a `:client_timeout` option that specifies 
  the milliseconds timeout passed to `GenServer.call`, and a `:message_timeout` 
  option that specifies how long the GenServer should wait for a response before 
  crashing (overriding the default value set in `DatomicGenServer.start` or 
  `DatomicGenServer.start_link`). Note that if the `:client_timeout` is shorter 
  than the `:message_timeout` value, the call will return an error but the server 
  will not crash even if the response message is never returned from the Clojure peer.

## Example

      query = [:find, Db.q?(:c), :where, [Db.q?(:c), Db.doc, "A person's name"]]
      Db.q(DatomicGenServer, query)

      #=> {:ok, #MapSet<['A']>}  # ASCII representation of ID 65
      
  """

  @spec q(GenServer.server, [Exdn.exdn], [query_option]) :: {:ok, term} | {:error, term}
  def q(server_identifier, exdn, options \\ []) do
    case Exdn.from_elixir(exdn) do
      {:ok, edn_str} -> 
        case DatomicGenServer.q(server_identifier, edn_str, options) do
          {:ok, reply_str} -> convert_query_response(reply_str, options)
          error -> error
        end
      parse_error -> parse_error
    end
  end

  @doc """
  Issues a transaction against a DatomicGenServer using a transaction 
  formulated as an Elixir list of maps. This transaction is translated to an edn
  string which is then passed to the Datomic `transact` API function. 
  
  The first parameter to this function is the pid or alias of the GenServer process;  
  the second is the transaction data. 
  
  The options keyword list may include a `:client_timeout` option that specifies 
  the milliseconds timeout passed to GenServer.call, and a `:message_timeout` 
  option that specifies how long the GenServer should wait for a response before 
  crashing (overriding the default value set in `DatomicGenServer.start` or 
  `DatomicGenServer.start_link`). Note that if the `:client_timeout` is shorter 
  than the `:message_timeout` value, the call will return an error but the server 
  will not crash even if the response message is never returned from the Clojure peer.
  
## Example
  
      data_to_add = [%{ 
          Db.id => Db.dbid(Db.schema_partition),
          Db.ident => :"person/name",
          Db.value_type => Db.type_string,
          Db.cardinality => Db.cardinality_one,
          Db.doc => "A person's name",
          Db.install_attribute => Db.schema_partition
      }]
      Db.transact(DatomicGenServer, data_to_add)
      
      # => {:ok, %DatomicGenServer.Db.DatomicTransaction{
              basis_t_before: 1001,   
              basis_t_after: 1002, 
              retracted_datoms: [],
              added_datoms: [
                %DatomicGenServer.Db.Datom{a: 50, added: true, e: 13194139534314, tx: 13194139534314, 
                    v: %Calendar.DateTime{abbr: "UTC", day: 15, hour: 3, min: 20, month: 2, sec: 1, std_off: 0, 
                                          timezone: "Etc/UTC", usec: 746000, utc_off: 0, year: 2016}},
                %DatomicGenServer.Db.Datom{a: 41, added: true, e: 65, tx: 13194139534314, v: 35},
                %DatomicGenServer.Db.Datom{a: 62, added: true, e: 65, tx: 13194139534314, v: "A person's name"},
                %DatomicGenServer.Db.Datom{a: 10, added: true, e: 65, tx: 13194139534314, v: :"person/name"},
                %DatomicGenServer.Db.Datom{a: 40, added: true, e: 65, tx: 13194139534314, v: 23},
                %DatomicGenServer.Db.Datom{a: 13, added: true, e: 0, tx: 13194139534314, v: 65}],
              tempids: %{-9223367638809264706 => 65}}}

  """
  @spec transact(GenServer.server, [Exdn.exdn], [DatomicGenServer.send_option]) :: {:ok, DatomicTransaction.t} | {:error, term}
  def transact(server_identifier, exdn, options \\ []) do
    case Exdn.from_elixir(exdn) do
      {:ok, edn_str} -> 
        case DatomicGenServer.transact(server_identifier, edn_str, options) do          
          {:ok, reply_str} -> 
              case Exdn.to_elixir(reply_str) do
                {:ok, exdn_result} -> transaction(exdn_result)
                error -> error
              end
          error -> error
        end
      parse_error -> parse_error
    end
  end
  
  @doc """
  Issues an `entity` call to that is passed to the Datomic `entity` API function. 
  
  The first parameter to this function is the pid or alias of the GenServer process; 
  the second is an edn string representing the parameter that is to be passed to 
  `entity`: either an entity id, an ident, or a lookup ref. The third parameter 
  is a list of atoms that represent the keys of the attributes you wish to fetch, 
  or `:all` if you want all the entity's attributes. 
  
  The options keyword list may include a `:client_timeout` option that specifies 
  the milliseconds timeout passed to GenServer.call, and a `:message_timeout` 
  option that specifies how long the GenServer should wait for a response before 
  crashing (overriding the default value set in `start` or `start_link`). Note 
  that if the `:client_timeout` is shorter than the `:message_timeout` value, 
  the call will return an error but the server will not crash even if the message 
  is never returned from the Clojure peer.
  
## Example
  
      Db.entity(DatomicGenServer, :"person/email")
      
      # => {ok, %{ Db.ident => :"person/email", 
                   Db.value_type => Db.type_string, 
                   Db.cardinality => Db.cardinality_one, 
                   Db.doc => "A person's email"}}

  """
  @spec entity(GenServer.server, [Exdn.exdn], [atom] | :all, [query_option]) :: {:ok, term} | {:error, term}
  def entity(server_identifier, exdn, attr_names \\ :all, options \\ []) do
    case Exdn.from_elixir(exdn) do
      {:ok, edn_str} -> 
        case DatomicGenServer.entity(server_identifier, edn_str, attr_names, options) do          
          {:ok, reply_str} -> convert_query_response(reply_str, options)
          error -> error
        end
      parse_error -> parse_error
    end
  end
  
  @doc """
  Issues a call to net.phobot.datomic/seed to seed a database using database 
  migration files and seed data files in edn format. The database is not dropped
  or recreated before migrating and seeding.
  
  The first parameter to this function is the pid or alias of the GenServer process; 
  the second is the path to the directory containing the migration files. The 
  third parameter is the path to a (different) directory containing the seed data 
  files. The migration files will be processed in the sort order of their directory, 
  and then the seed data files in the sort order of their directory. 
  
  Seed data is loaded in a single transaction. The return value of the function 
  is the result of the Datomic `transact` API function call that executed the
  transaction, wrapped in a `DatomicTransaction` struct.
  
  The Clojure Conformity library is used to keep the migrations idempotent, but
  the loading of seed data is not idempotent.
  
  The options keyword list may include a `:client_timeout` option that specifies 
  the milliseconds timeout passed to GenServer.call, and a `:message_timeout` 
  option that specifies how long the GenServer should wait for a response before 
  crashing (overriding the default value set in `start` or `start_link`). Note 
  that if the `:client_timeout` is shorter than the `:message_timeout` value, 
  the call will return an error but the server will not crash even if the message 
  is never returned from the Clojure peer.
  
## Example
      migration_dir = Path.join [System.cwd(), "migrations"]
      seed_data_dir = Path.join [System.cwd(), "seed-data"]
      DatomicGenServer.seed(DatomicGenServer, migration_dir, seed_data_dir)
      
      => {:ok, "{:db-before {:basis-t 1000}, :db-after {:basis-t 1000}, ...
      
  """
  @spec seed(GenServer.server, String.t, String.t, [DatomicGenServer.send_option]) :: {:ok, DatomicTransaction.t} | {:error, term}
  def seed(server_identifier, migration_path, seed_data_path, options \\ []) do
    case DatomicGenServer.seed(server_identifier, migration_path, seed_data_path, options) do          
      {:ok, reply_str} -> 
          case Exdn.to_elixir(reply_str) do
            {:ok, exdn_result} -> transaction(exdn_result)
            error -> error
          end
      error -> error
    end
  end
  
  @spec convert_query_response(String.t, [query_option]) :: {:ok, term} | {:error, term}
  defp convert_query_response(response_str, options) do
    converter = Keyword.get(options, :response_converter) || (fn x -> x end)
    handlers = Keyword.get(options, :edn_tag_handlers) || Exdn.standard_handlers
    Exdn.to_elixir(response_str, converter, handlers)
  end
  
############################# DATOMIC SHORTCUTS  ############################
  # Id/ident
  @doc "Convenience function that generates `#db/id[ <partition> ]`"
  @spec dbid(atom) :: {:tag, :"db/id", [atom]} 
  def dbid(db_part) do
    {:tag, :"db/id", [db_part]}
  end
  
  @doc "Convenience shortcut for `:\"db/id\"`"
  @spec id :: :"db/id"
  def id, do: :"db/id"
  
  @doc "Convenience shortcut for `:\"db/ident\"`"
  @spec ident :: :"db/ident"
  def ident, do: :"db/ident"

  # Transaction creation
  @doc "Convenience shortcut for `:\"db/add\"`"
  @spec add :: :"db/add"
  def add, do: :"db/add"
  
  @doc "Convenience shortcut for `:\"db/retract\"`"
  @spec retract :: :"db/retract"
  def retract, do: :"db/retract"
  
  @doc "Convenience shortcut for `:\"db.install/_attribute\"`"
  @spec install_attribute :: :"db.install/_attribute"
  def install_attribute, do:  :"db.install/_attribute"
  
  @doc "Convenience shortcut for `:\"db.alter/attribute\"`"
  @spec alter_attribute :: :"db.alter/attribute"
  def alter_attribute, do: :"db.alter/attribute"
  
  @doc "Convenience shortcut for `:\"db/txInstant\"`"
  @spec tx_instant :: :"db/txInstant"
  def tx_instant, do: :"db/txInstant"

  # Value types
  @doc "Convenience shortcut for `:\"db/valueType\"`"
  @spec value_type :: :"db/valueType"
  def value_type, do: :"db/valueType"
  
  @doc "Convenience shortcut for `:\"db.type/long\"`"
  @spec type_long :: :"db.type/long"
  def type_long, do: :"db.type/long"
  
  @doc "Convenience shortcut for `:\"db.type/keyword\"`"
  @spec type_keyword :: :"db.type/keyword"
  def type_keyword, do:  :"db.type/keyword"
  
  @doc "Convenience shortcut for `:\"db.type/string\"`"
  @spec type_string :: :"db.type/string"
  def type_string, do: :"db.type/string"
  
  @doc "Convenience shortcut for `:\"db.type/boolean\"`"
  @spec type_boolean :: :"db.type/boolean"
  def type_boolean, do: :"db.type/boolean"
  
  @doc "Convenience shortcut for `:\"db.type/bigint\"`"
  @spec type_bigint :: :"db.type/bigint"
  def type_bigint, do: :"db.type/bigint"
  
  @doc "Convenience shortcut for `:\"db.type/float\"`"
  @spec type_float :: :"db.type/float"
  def type_float, do: :"db.type/float"
  
  @doc "Convenience shortcut for `:\"db.type/double\"`"
  @spec type_double :: :"db.type/double"
  def type_double, do: :"db.type/double"
  
  @doc "Convenience shortcut for `:\"db.type/bigdec\"`"
  @spec type_bigdec :: :"db.type/bigdec"
  def type_bigdec, do: :"db.type/bigdec"
  
  @doc "Convenience shortcut for `:\"db.type/ref\"`"
  @spec type_ref :: :"db.type/ref"
  def type_ref, do: :"db.type/ref"
  
  @doc "Convenience shortcut for `:\"db.type/instant\"`"
  @spec type_instant :: :"db.type/instant"
  def type_instant, do: :"db.type/instant"
  
  @doc "Convenience shortcut for `:\"db.type/uuid\"`"
  @spec type_uuid :: :"db.type/uuid"
  def type_uuid, do: :"db.type/uuid"
  
  @doc "Convenience shortcut for `:\"db.type/uri\"`"
  @spec type_uri :: :"db.type/uri"
  def type_uri, do: :"db.type/uri"
  
  @doc "Convenience shortcut for `:\"db.type/bytes\"`"
  @spec type_bytes :: :"db.type/bytes"
  def type_bytes, do: :"db.type/bytes"

  # Cardinalities
  @doc "Convenience shortcut for `:\"db/cardinality\"`"
  @spec cardinality :: :"db/cardinality"
  def cardinality, do: :"db/cardinality"
  
  @doc "Convenience shortcut for `:\"db.cardinality/one\"`"
  @spec cardinality_one :: :"db.cardinality/one"
  def cardinality_one, do:  :"db.cardinality/one"
  
  @doc "Convenience shortcut for `:\"db.cardinality/many\"`"
  @spec cardinality_many :: :"db.cardinality/many"
  def cardinality_many, do: :"db.cardinality/many"
  
  # Optional Schema Attributes  
  @doc "Convenience shortcut for `:\"db/doc\"`"
  @spec doc :: :"db/doc"
  def doc, do: :"db/doc"
  
  @doc "Convenience shortcut for `:\"db/unique\"`"
  @spec unique :: :"db/unique"
  def unique, do: :"db/unique"
  
  @doc "Convenience shortcut for `:\"db.unique/value\"`"
  @spec unique_value :: :"db.unique/value"
  def unique_value, do: :"db.unique/value"
  
  @doc "Convenience shortcut for `:\"db.unique/identity\"`"
  @spec unique_identity :: :"db.unique/identity"
  def unique_identity, do: :"db.unique/identity"
  
  @doc "Convenience shortcut for `:\"db/index\"`"
  @spec index :: :"db/index"
  def index, do: :"db/index"
  
  @doc "Convenience shortcut for `:\"db/fulltext\"`"
  @spec fulltext :: :"db/fulltext"
  def fulltext, do: :"db/fulltext"
  
  @doc "Convenience shortcut for `:\"db/isComponent\"`"
  @spec is_component :: :"db/isComponent"
  def is_component, do: :"db/isComponent"
  
  @doc "Convenience shortcut for `:\"db/noHistory\"`"
  @spec no_history :: :"db/noHistory"
  def no_history, do: :"db/noHistory"
  
  # Functions  
  @doc "Convenience shortcut for `:\"db/fn\"`"
  @spec _fn :: :"db/fn"
  def _fn, do: :"db/fn"
  
  @doc "Convenience shortcut for `:\"db.fn/retractEntity\"`"
  @spec fn_retract_entity :: :"db.fn/retractEntity"
  def fn_retract_entity, do: :"db.fn/retractEntity"
  
  @doc "Convenience shortcut for `:\"db.fn/cas\"`"
  @spec fn_cas :: :"db.fn/cas"
  def fn_cas, do: :"db.fn/cas"

  # Common partions
  @doc "Convenience shortcut for `:\"db.part/db\"`"
  @spec schema_partition :: :"db.part/db"
  def schema_partition, do: :"db.part/db"
  
  @doc "Convenience shortcut for `:\"db.part/tx\"`"
  @spec transaction_partition :: :"db.part/tx"
  def transaction_partition, do: :"db.part/tx"
  
  @doc "Convenience shortcut for `:\"db.part/user\"`"
  @spec user_partition :: :"db.part/user"
  def user_partition, do: :"db.part/user"

  # Query placeholders
  @doc """
  Convenience function to generate Datomic query placeholders--i.e., 
  symbols prefixed by a question mark. 
  
  Accepts an atom as its argument, representing the symbol to which 
  the question mark is to be prepended.
  """  
  @spec q?(atom) :: {:symbol, atom }
  def q?(placeholder_atom) do
    variable_symbol = placeholder_atom |> to_string
    with_question_mark = "?" <> variable_symbol |> String.to_atom
    {:symbol, with_question_mark }
  end

  # Data sources
  @doc "Convenience shortcut for the implicit data source `$`"
  @spec implicit :: {:symbol, :"$"}
  def implicit, do: {:symbol, :"$"}
  
  @doc """
  Convenience function to generate Datomic data source specifications--i.e., 
  symbols prefixed by a dollar sign. 
  
  Accepts an atom as its argument, representing the symbol to which the dollar 
  sign is to be prepended.
  """  
  @spec inS(atom) :: {:symbol, atom }
  def inS(placeholder_atom) do
    placeholder = placeholder_atom |> to_string
    with_dollar_sign = "$" <> placeholder |> String.to_atom
    {:symbol, with_dollar_sign }    
  end

  # Bindings and find specifications
  @doc """
  Convenience shortcut for the single scalar find specification `.`
  as used, for example, in: `[:find ?e . :where [?e age 42] ]`

  """
  @spec single_scalar :: {:symbol, :"."}
  def single_scalar, do: {:symbol, :"."}

  @doc """
  Convenience shortcut for the blank binding `_` as used, for example, in: 
  `[:find ?x :where [_ :likes ?x]]`

  """
  @spec blank_binding :: {:symbol, :"_"}
  def blank_binding, do: {:symbol, :"_"}

  @doc """
  Convenience shortcut for collection binding find specification `...`
  as used, for example, in: `[:find ?e in $ [?a ...] :where [?e age ?a] ]`

  """
  @spec collection_binding(atom) :: [{:symbol, atom},...]
  def collection_binding(placeholder_atom) do
    [ q?(placeholder_atom), {:symbol, :"..."} ]
  end

  # Clauses
  @doc """
  Convenience shortcut for creating a `not` clause.
  
  In Exdn, Clojure lists are represented as tuples with the tag `:list`, so this 
  allows us not to have to sprinkle that syntax all over the place.

## Example

      Db._not([Db.q?(:eid), :"person/age" 13])
      
  sends the following to Datomic:

      (not [?eid :person/age 13])

  """  
  @spec _not([Exdn.exdn]) :: {:list, [Exdn.exdn]}
  def _not(inner_clause), do: datomic_expression(:not, [inner_clause])

  @doc """
  Convenience shortcut for creating a `not-join` clause.

  In Exdn, Clojure lists are represented as tuples with the tag `:list`, so this 
  function allows us not to have to sprinkle that syntax all over the place.
  
## Example

      Db._not_join(
        [ Db.q?(:employer) ],
        [ [Db.q?(:employer), :"business/employee" Db.q?(:person)],
          [Db.q?(:employer), :"business/nonprofit" true]
        ]
      )
      
  sends the following to Datomic:

      (not-join [?employer]
             [?employer :business/employee ?person]
             [?employer :business/nonprofit true])

  """  
  @spec _not_join([{:symbol, atom},...], [Exdn.exdn]) :: {:list, [Exdn.exdn]}
  def _not_join(binding_list, inner_clause_list) do
    clauses_including_bindings = [ binding_list | inner_clause_list ]
    datomic_expression(:"not-join", clauses_including_bindings)
  end

  @doc """
  Convenience shortcut for creating an `or` clause.
  
  In Exdn, Clojure lists are represented as tuples with the tag `:list`, so this 
  function allows us not to have to sprinkle that syntax all over the place.
    
## Example

      Db._or([
          [Db.q?(:org), :"business/nonprofit" true],
          [Db.q?(:org), :"organization/ngo" true]
      ])
      
  sends the following to Datomic:
          
      (or [?org :business/nonprofit true]
          [?org :organization/ngo true])

  """  
  @spec _or([Exdn.exdn]) :: {:list, [Exdn.exdn]}
  def _or(inner_clauses), do: datomic_expression(:or, inner_clauses)

  @doc """
  Convenience shortcut for creating an `and` clause.
  
  Note that in Datomic, `and` clauses are only for use inside `or` clauses; `and` 
  is the default otherwise.

  In Exdn, Clojure lists are represented as tuples with the tag `:list`, so this 
  function allows us not to have to sprinkle that syntax all over the place.

## Example

      Db._and([
          [Db.q?(:org), :"organization/ngo" true],
          [Db.q?(:org), :"organization/country" :"country/france"]
      ])

  sends the following to Datomic:
    
      (and [?org :organization/ngo true]
           [?org :organization/country :country/france])

  """
  @spec _and([Exdn.exdn]) :: {:list, [Exdn.exdn]}
  def _and(inner_clauses), do: datomic_expression(:and, inner_clauses)

  @doc """
  Convenience shortcut for creating an `or-join` clause.
  
  The first parameter to this function should be a list of bindings; the second
  the list of clauses.
    
  In Exdn, Clojure lists are represented as tuples with the tag `:list`, so this 
  function allows us not to have to sprinkle that syntax all over the place.
  
## Example

      Db._or_join(
        [ Db.q?(:person) ],
        [ Db._and([
            [Db.q?(:employer), :"business/employee" Db.q?(:person)],
            [Db.q?(:employer), :"business/nonprofit" true]
          ]),
          [Db.q?(:person), :"person/age" 65]
        ]
      )
      
  sends the following to Datomic:
          
      (or-join [?person]
             (and [?employer :business/employee ?person]
                  [?employer :business/nonprofit true]))
             [?person :person/age 65])

  """
  @spec _or_join([{:symbol, atom},...], [Exdn.exdn]) :: {:list, [Exdn.exdn]}
  def _or_join(binding_list, inner_clause_list) do
    clauses_including_bindings = [ binding_list | inner_clause_list ]
    datomic_expression(:"or-join", clauses_including_bindings)
  end

  @doc """
  Convenience shortcut for creating a Datomic pull expression.
  
  In Exdn, Clojure lists are represented as tuples with the tag `:list`, so this 
  function allows us not to have to sprinkle that syntax all over the place.
  
## Example

      Db._pull(Db.q?(:e), [:"person/address"])
      
  sends the following to Datomic:
          
      (pull ?e [:person/address])

  """
  @spec _pull({:symbol, atom}, [Exdn.exdn]) :: {:list, [Exdn.exdn]}
  def _pull({:symbol, entity_var}, pattern_clauses) do
    datomic_expression(:pull, [entity_var, pattern_clauses])
  end

  @doc """
  Convenience shortcut for creating a Datomic expression clause. 
  
  An expression clause allows arbitrary Java or Clojure functions to be used 
  inside of Datalog queries; they are either of form `[(predicate ...)]` or 
  `[(function ...) bindings]`. An expression clause is thus a Clojure list inside 
  a vector.

  In Exdn, Clojure lists are represented as tuples with the tag `:list`, so this 
  function allows us not to have to sprinkle that syntax all over the place.
  """
  @spec _expr(atom, [Exdn.exdn], [Exdn.exdn]) :: [{:list, [Exdn.exdn]}]
  def _expr(function_symbol, remaining_expressions, bindings) do
    [ datomic_expression(function_symbol, remaining_expressions) | bindings ]
  end

  # An expression is a list starting with a symbol
  @spec datomic_expression(atom, [Exdn.exdn]) :: {:list, [Exdn.exdn]}
  defp datomic_expression(symbol_atom, remaining_expressions) do
    clause_list = [{:symbol, symbol_atom} | remaining_expressions ]
    {:list, clause_list}
  end

########## PRIVATE FUNCTIONS FOR STRUCTIFYING TRANSACTION RESPONSES #############
  @spec transaction(transaction_result) :: {:ok, DatomicTransaction.t} | {:error, term}
  defp transaction(transaction_result) do
    try do
      {added_datoms, retracted_datoms} = tx_data(transaction_result) |> to_datoms
      transaction_struct = %DatomicTransaction{
                              basis_t_before: basis_t_before(transaction_result), 
                              basis_t_after: basis_t_after(transaction_result), 
                              added_datoms: added_datoms, 
                              retracted_datoms: retracted_datoms, 
                              tempids: tempids(transaction_result)}
      {:ok, transaction_struct}
    rescue
      e -> {:error, e}
    end
  end
  
  @spec to_datoms([datom_map]) :: {[Datom.t], [Datom.t]}
  defp to_datoms(datom_maps) do
    datom_maps
    |> Enum.map(fn(datom_map) -> struct(Datom, datom_map) end) 
    |> Enum.partition(fn(datom) -> datom.added end)
  end
  
  @spec basis_t_before(%{:"db-before" => %{:"basis-t" => integer}}) :: integer
  defp basis_t_before(%{:"db-before" => %{:"basis-t" => before_t}}) do
    before_t
  end
  
  @spec basis_t_after(%{:"db-after" => %{:"basis-t" => integer}}) :: integer
  defp basis_t_after(%{:"db-after" => %{:"basis-t" => after_t}}) do
    after_t
  end
  
  @spec tx_data(%{:"tx-data" => [datom_map]}) :: [datom_map]
  defp tx_data(%{:"tx-data" => tx_data}) do
    tx_data
  end
  
  @spec tempids(%{tempids: %{integer => integer}}) :: %{integer => integer}
  defp tempids(%{tempids: tempids}) do
    tempids
  end
end
