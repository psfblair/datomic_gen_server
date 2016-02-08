defmodule DatomicGenServer.Db do
  @type edn :: atom | boolean | number | String.t | tuple | [edn] | %{edn => edn} | MapSet.t(edn) 
  
  # Interface functions to the GenServer
  @spec q([edn], non_neg_integer | nil, non_neg_integer | nil) :: term
  def q(edn, message_timeout_millis \\ nil, timeout_on_call \\ nil) do
    case Exdn.from_elixir(edn) do
      {:ok, edn_str} -> 
        case DatomicGenServer.q(edn_str, message_timeout_millis, timeout_on_call) do
          %{ok: reply_str} -> Exdn.to_elixir(reply_str)
          error -> error
        end
      parse_error -> parse_error
    end
  end
  
  @spec transact([edn], non_neg_integer | nil, non_neg_integer | nil) :: term
  def transact(edn, message_timeout_millis \\ nil, timeout_on_call \\ nil) do
    case Exdn.from_elixir(edn) do
      {:ok, edn_str} -> 
        case DatomicGenServer.transact(edn_str, message_timeout_millis, timeout_on_call) do
          {:ok, reply_str} -> Exdn.to_elixir(reply_str)
          error -> error
        end
      parse_error -> parse_error
    end
  end

  # Id/ident
  @spec id :: :"db/id"
  def id, do: :"db/id"
  
  @spec ident :: :"db/ident"
  def ident, do: :"db/ident"

  # Transaction creation
  @spec add :: :"db/add"
  def add, do: :"db/add"
  
  @spec retract :: :"db/retract"
  def retract, do: :"db/retract"
  
  @spec install_attribute :: :"db.install/_attribute"
  def install_attribute, do:  :"db.install/_attribute"
  
  @spec alter_attribute :: :"db.alter/attribute"
  def alter_attribute, do: :"db.alter/attribute"
  
  @spec tx_instant :: :"db/txInstant"
  def tx_instant, do: :"db/txInstant"

  # Value types
  @spec value_type :: :"db/valueType"
  def value_type, do: :"db/valueType"
  
  @spec type_long :: :"db.type/long"
  def type_long, do: :"db.type/long"
  
  @spec type_keyword :: :"db.type/keyword"
  def type_keyword, do:  :"db.type/keyword"
  
  @spec type_string :: :"db.type/string"
  def type_string, do: :"db.type/string"
  
  @spec type_boolean :: :"db.type/boolean"
  def type_boolean, do: :"db.type/boolean"
  
  @spec type_bigint :: :"db.type/bigint"
  def type_bigint, do: :"db.type/bigint"
  
  @spec type_float :: :"db.type/float"
  def type_float, do: :"db.type/float"
  
  @spec type_double :: :"db.type/double"
  def type_double, do: :"db.type/double"
  
  @spec type_bigdec :: :"db.type/bigdec"
  def type_bigdec, do: :"db.type/bigdec"
  
  @spec type_ref :: :"db.type/ref"
  def type_ref, do: :"db.type/ref"
  
  @spec type_instant :: :"db.type/instant"
  def type_instant, do: :"db.type/instant"
  
  @spec type_uuid :: :"db.type/uuid"
  def type_uuid, do: :"db.type/uuid"
  
  @spec type_uri :: :"db.type/uri"
  def type_uri, do: :"db.type/uri"
  
  @spec type_bytes :: :"db.type/bytes"
  def type_bytes, do: :"db.type/bytes"

  # Cardinalities
  @spec cardinality :: :"db/cardinality"
  def cardinality, do: :"db/cardinality"
  
  @spec cardinality_one :: :"db.cardinality/one"
  def cardinality_one, do:  :"db.cardinality/one"
  
  @spec cardinality_many :: :"db.cardinality/many"
  def cardinality_many, do: :"db.cardinality/many"
  
  # Optional Schema Attributes  
  @spec doc :: :"db/doc"
  def doc, do: :"db/doc"
  
  @spec unique :: :"db/unique"
  def unique, do: :"db/unique"
  
  @spec unique_value :: :"db.unique/value"
  def unique_value, do: :"db.unique/value"
  
  @spec unique_identity :: :"db.unique/identity"
  def unique_identity, do: :"db.unique/identity"
  
  @spec index :: :"db/index"
  def index, do: :"db/index"
  
  @spec fulltext :: :"db/fulltext"
  def fulltext, do: :"db/fulltext"
  
  @spec is_component :: :"db/isComponent"
  def is_component, do: :"db/isComponent"
  
  @spec no_history :: :"db/noHistory"
  def no_history, do: :"db/noHistory"
  
  # Keys used in transaction responses
  @spec tx_data :: :"tx-data"
  def tx_data, do: :"tx-data"

  @spec db_after :: :"db-after"
  def db_after, do: :"db-after"
  
  @spec db_before :: :"db-before"
  def db_before, do: :"db-before"
  
  @spec db_alias :: :"db/alias"
  def db_alias, do: :"db/alias"
  
  @spec basis_t :: :"basis-t"
  def basis_t, do: :"basis-t"
  
  @spec tempids :: :"tempids"
  def tempids, do: :"tempids"

  @spec a :: :a
  def a, do: :a
  
  @spec e :: :e
  def e, do: :e
  
  @spec v :: :v
  def v, do: :v
  
  @spec tx :: :tx
  def tx, do: :tx

  @spec added :: :added
  def added, do: :added

  # Functions  
  @spec _fn :: :"db/fn"
  def _fn, do: :"db/fn"
  
  @spec fn_retract_entity :: :"db.fn/retractEntity"
  def fn_retract_entity, do: :"db.fn/retractEntity"
  
  @spec fn_cas :: :"db.fn/cas"
  def fn_cas, do: :"db.fn/cas"

  # Common partions
  @spec schema_partition :: :"db.part/db"
  def schema_partition, do: :"db.part/db"
  
  @spec transaction_partition :: :"db.part/tx"
  def transaction_partition, do: :"db.part/tx"
  
  @spec user_partition :: :"db.part/user"
  def user_partition, do: :"db.part/user"

  # Query placeholders
  # Symbol containing ? prefixed
  @spec q?(atom) :: {:symbol, atom }
  def q?(placeholder_atom) do
    variable_symbol = placeholder_atom |> to_string
    with_question_mark = "?" <> variable_symbol |> String.to_atom
    {:symbol, with_question_mark }
  end

  # Data sources
  # Implicit data source - $
  @spec implicit :: {:symbol, :"$"}
  def implicit, do: {:symbol, :"$"}
  
  # Symbol containing $ prefixed for data source specification
  @spec inS(atom) :: {:symbol, atom }
  def inS(placeholder_atom) do
    placeholder = placeholder_atom |> to_string
    with_dollar_sign = "$" <> placeholder |> String.to_atom
    {:symbol, with_dollar_sign }    
  end

  # Bindings and find specifications
  # For use in [:find ?e . :where [?e age 42] ]
  @spec single_scalar :: {:symbol, :"."}
  def single_scalar, do: {:symbol, :"."}

  # For use in [:find ?x :where [_ :likes ?x]]
  @spec blank_binding :: {:symbol, :"_"}
  def blank_binding, do: {:symbol, :"_"}

  # [?atom ...]
  @spec collection_binding(atom) :: [{:symbol, atom},...]
  def collection_binding(placeholder_atom) do
    [ q?(placeholder_atom), {:symbol, :"..."} ]
  end

  # Clauses
  @spec not_clause([edn]) :: {:list, [edn]}
  def not_clause(inner_clause), do: datomic_expression(:not, [inner_clause])

  @spec not_join_clause([{:symbol, atom},...], [edn]) :: {:list, [edn]}
  def not_join_clause(binding_list, inner_clause_list) do
    clauses_including_bindings = [ binding_list | inner_clause_list ]
    datomic_expression(:"not-join", clauses_including_bindings)
  end

  @spec or_clause([edn]) :: {:list, [edn]}
  def or_clause(inner_clauses), do: datomic_expression(:or, inner_clauses)

  # Only for use inside or clauses; `and` is the default otherwise.
  @spec and_clause([edn]) :: {:list, [edn]}
  def and_clause(inner_clauses), do: datomic_expression(:and, inner_clauses)

  @spec or_join_clause([{:symbol, atom},...], [edn]) :: {:list, [edn]}
  def or_join_clause(binding_list, inner_clause_list) do
    clauses_including_bindings = [ binding_list | inner_clause_list ]
    datomic_expression(:"or-join", clauses_including_bindings)
  end

  @spec pull_expression({:symbol, atom}, [edn]) :: {:list, [edn]}
  def pull_expression(entity_var, pattern_clauses) do
    datomic_expression(:pull, [entity_var, pattern_clauses])
  end

  # An expression clause is a Clojure list inside a vector.
  @spec expression_clause(atom, [edn]) :: [{:list, [edn]}]
  def expression_clause(function_symbol, remaining_expressions) do
    [ datomic_expression(function_symbol, remaining_expressions) ]
  end

  # An expression is a list starting with a symbol
  @spec datomic_expression(atom, [edn]) :: {:list, [edn]}
  defp datomic_expression(symbol_atom, remaining_expressions) do
    clause_list = [{:symbol, symbol_atom} | remaining_expressions ]
    {:list, clause_list}
  end
end
