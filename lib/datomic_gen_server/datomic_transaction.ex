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
