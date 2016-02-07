defmodule DatomicGenServer.DbTest do
  use ExUnit.Case, async: false
  alias DatomicGenServer.Db, as: Db
  
  setup do
    # Need a long timeout until the JVM actually starts.
    DatomicGenServer.start_link("datomic:mem://test", true, 20_000)
    :ok
  end

  test "issues Datomic queries" do
    query = [:find, Db.q?(:c), :where, [Db.q?(:c), :"db/doc", "A person's name"]]
    result = Db.q(query)
    second_result = Db.q(query)
    assert {:ok, MapSet.new()} == result
    assert {:ok, MapSet.new()} == second_result
  end
end
