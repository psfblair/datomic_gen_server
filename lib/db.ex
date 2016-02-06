defmodule Db do

  def add, do: :"db/add"
  def retract, do: :"db/retract"
  def install_attribute, do:  :"db.install/_attribute"
  def alter_attribute, do: :"db.alter/attribute"
  
  def tx_instant, do: :"db/txInstant"

  def id, do: :"db/id"
  def ident, do: :"db/ident"

  # Value types
  def value_type, do: :"db/valueType"
  def type_long, do: :"db.type/long"
  def type_keyword, do:  :"db.type/keyword "
  def type_string, do: :"db.type/string"
  def type_boolean, do: :"db.type/boolean"
  def type_bigint, do: :"db.type/bigint"
  def type_float, do: :"db.type/float"
  def type_double, do: :"db.type/double"
  def type_bigdec, do: :"db.type/bigdec"
  def type_ref, do: :"db.type/ref"
  def type_instant, do: :"db.type/instant"
  def type_uuid, do: :"db.type/uuid"
  def type_uri, do: :"db.type/uri"
  def type_bytes, do: :"db.type/bytes"

  # Cardinalities
  def cardinality, do: :"db/cardinality"
  def cardinality_one, do:  :"db.cardinality/one"
  def cardinality_many, do: :"db.cardinality/many"
  
  # Optional Schema Attributes
  def doc, do: :"db/doc"
  def unique, do: :"db/unique"
  def unique_value, do: :"db.unique/value"
  def unique_identity, do: :"db.unique/identity"
  def index, do: :"db/index"
  def fulltext, do: :"db/fulltext"
  def is_component, do: :"db/isComponent"
  def no_history, do: :"db/noHistory"
  
  # Functions
  def _fn, do: :"db/fn"
  def fn_retract_entity, do: :"db.fn/retractEntity"
  def fn_cas, do: :"db.fn/cas"

  # Partions
  def schema_partition, do: :"db.part/db"
  def transaction_partition, do: :"db.part/tx"
  def user_partition, do: :"db.part/user"

  # Keys in transaction responses
  def a, do: :a
  def e, do: :e
  def v, do: :v
  def tx, do: :tx
  def added, do: :added

  def db_alias, do: :"db/alias"

  def tx_data, do: :"tx-data"
  def basis_t, do: :"basis-t"

  def db_after, do: :"db-after"
  def db_before, do: :"db-before"
  def tempids, do: :"tempids"

  # Data sources
  # Implicit data source - $
  def implicit, do: {:symbol, :"$"}

  # Query placeholders
  def q?(placeholder_atom) do
    variable_symbol = placeholder_atom |> to_string
    with_question_mark = "?" <> variable_symbol |> String.to_atom
    {:symbol, with_question_mark }
  end

  # Bindings and find specifications
  # For use in [:find ?e . :where [?e age 42] ]
  def single_scalar, do: {:symbol, :"."}

  # For use in [:find ?x :where [_ :likes ?x]]
  def blank_binding, do: {:symbol, :"_"}

  # [?atom ...]
  def collection_binding(placeholder_atom) do
    [ q?(placeholder_atom), {:symbol, :"..."} ]
  end

  # Clauses
  def not_clause(inner_clause), do: datomic_expression(:not, [inner_clause])

  def not_join_clause(binding_list, inner_clause_list) do
    clauses_including_bindings = [ binding_list | inner_clause_list ]
    datomic_expression(:"not-join", clauses_including_bindings)
  end

  def or_clause(inner_clauses), do: datomic_expression(:or, inner_clauses)

  # Only for use inside and clauses; `and` is the default otherwise.
  def and_clause(inner_clauses), do: datomic_expression(:and, inner_clauses)

  def or_join_clause(binding_list, inner_clause_list) do
    clauses_including_bindings = [ binding_list | inner_clause_list ]
    datomic_expression(:"or-join", clauses_including_bindings)
  end

  def pull_expression(entity_var, pattern_clauses) do
    datomic_expression(:pull, [entity_var, pattern_clauses])
  end

  # An expression clause is a Clojure list inside a vector.
  def expression_clause(function_symbol, remaining_expressions) do
    [ datomic_expression(function_symbol, remaining_expressions) ]
  end

  # An expression is a list starting with a symbol
  defp datomic_expression(symbol_atom, remaining_expressions) do
    clause_list = [{:symbol, symbol_atom} | remaining_expressions ]
    {:list, clause_list}
  end

end
