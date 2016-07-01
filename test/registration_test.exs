defmodule RegistrationTest do
  use ExUnit.Case, async: false
  
  # Need long timeouts to let the JVM start.
  test "can register global name" do
    DatomicGenServer.start_link(
      "datomic:mem://test", 
      true, 
      [{:timeout, 20_000}, {:default_message_timeout, 20_000}, {:name, {:global, :foo}}]
    )
    :global.send(:foo, {:EXIT, self, :normal})  
  end
  
  test "can register local name" do
    DatomicGenServer.start_link(
      "datomic:mem://test", 
      true, 
      [{:timeout, 20_000}, {:default_message_timeout, 20_000}, {:name, RegistrationTest}]
    )    
    send(RegistrationTest, {:EXIT, self, :normal})
  end
  
  test "can start without registering a name" do
    {:ok, pid} = 
      DatomicGenServer.start_link(
        "datomic:mem://test", 
        true, 
        [{:timeout, 20_000}, {:default_message_timeout, 20_000}]
      )
    send(pid, {:EXIT, self, :normal})
  end
end
