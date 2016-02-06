defmodule DatomicGenServerTest do
  use ExUnit.Case
  
  setup do 
    on_exit fn ->
      try do
        DatomicGenServer.exit()
      catch
        :exit, _ -> :ok
      end
    end
    
    # Need a long timeout until the JVM actually starts.
    DatomicGenServer.start_link("datomic:mem://test", true, 20_000)
    :ok
  end
  
  test "issues Datomic queries" do
    query = [:find, Db.q?(:c), :where, [Db.q?(:c), :"db/doc", "A person's name"]]
    result = DatomicGenServer.q(query)
    second_result = DatomicGenServer.q(query)
    assert {:ok, MapSet.new()} == result
    assert {:ok, MapSet.new()} == second_result
  end
end
