defmodule Datom do
  @moduledoc """
  A data structure for representing a Datomic datom.
  """
  defstruct e: 0, a: 0, v: [], tx: %{}, added: false
  @type t :: %Datom{e: integer, a: atom, v: term, tx: integer, added: boolean}
end
