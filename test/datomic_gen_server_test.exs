defmodule DatomicGenServerTest do
  use ExUnit.Case, async: false
  
  setup do
    # Need a long timeout until the JVM actually starts.
    DatomicGenServer.start_link("datomic:mem://test", true, 20_000)
    :ok
  end
  
  test "issues Datomic queries" do
    query = "[:find ?c :where [?c :db/doc \"A person's name\"]]"
    result = DatomicGenServer.q(query)
    second_result = DatomicGenServer.q(query)
    assert %{ok: "\#{}\n"} == result
    assert %{ok: "\#{}\n"} == second_result
  end
end
