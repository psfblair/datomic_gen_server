defmodule Datom do
  defstruct e: 0, a: 0, v: [], tx: %{}, added: false
  @type t :: %Datom{e: integer, a: atom, v: term, tx: integer, added: boolean}
end
