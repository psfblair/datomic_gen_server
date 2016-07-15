defmodule DatomicGenServerTest do
  use ExUnit.Case, async: false
  alias DatomicGenServer.Db, as: Db
  
  setup_all do
    # Need long timeouts to let the JVM start.
    DatomicGenServer.start_link(
      "datomic:mem://test", 
      true, 
      [{:timeout, 20_000}, {:default_message_timeout, 20_000}, {:name, DatomicGenServer}]
    )
    :ok
  end
  
  test "can issue multiple Datomic queries" do
    query = "[:find ?c :where [?c :db/doc \"Some docstring that shouldn't be in the database\"]]"
    result = DatomicGenServer.q(DatomicGenServer, query)
    assert {:ok, "\#{}\n"} == result
    second_result = DatomicGenServer.q(DatomicGenServer, query)
    assert {:ok, "\#{}\n"} == second_result
  end
  
  test "can issue parameterized queries" do
    query = "[:find ?c :in $ ?docstring :where [?c :db/doc ?docstring]]"
    result = DatomicGenServer.q(DatomicGenServer, query, 
      ["datomic_gen_server.peer/*db*", "\"Some docstring that shouldn't be in the database\""]
    )
    assert {:ok, "\#{}\n"} == result
  end

  test "can execute Datomic transactions" do
    data_to_add = """
      [ { :db/id #db/id[:db.part/db]
          :db/ident :person/name
          :db/valueType :db.type/string
          :db/cardinality :db.cardinality/one
          :db/doc \"A person's name\"
          :db.install/_attribute :db.part/db}]
    """
    {:ok, transaction_result} = DatomicGenServer.transact(DatomicGenServer, data_to_add)
    assert Regex.match?(~r/:db-before \{:basis-t \d+/, transaction_result)
    assert Regex.match?(~r/:db-after \{:basis-t \d+/, transaction_result)
    assert Regex.match?(~r/:tx-data \[\{:a \d+/, transaction_result)
    assert Regex.match?(~r/:tempids \{/, transaction_result)

    query = "[:find ?c :where [?c :db/doc \"A person's name\"]]"
    {:ok, result_str} = DatomicGenServer.q(DatomicGenServer, query)
    assert Regex.match?(~r/\#\{\[\d+\]\}\n/, result_str)
  end
  
  test "will evaluate unescaped bindings" do
    data_to_add = """
      [ { :db/id #db/id[:db.part/db]
          :db/ident :person/name
          :db/valueType :db.type/string
          :db/cardinality :db.cardinality/one
          :db/doc \"A person's name\"
          :db.install/_attribute :db.part/db}]
    """
    {:ok, _} = DatomicGenServer.transact(DatomicGenServer, data_to_add)

    query = "[:find ?e :in $ ?idmin :where [?e :db/ident :person/name][(> ?e ?idmin)]]"
    {:ok, result_str} = DatomicGenServer.q(DatomicGenServer, query, 
                            ["datomic_gen_server.peer/*db*", "(- 1 1)"])
                            
    assert Regex.match?(~r/\#\{\[\d+\]\}\n/, result_str)
  end
  
  test "does not evaluate escaped bindings" do
    data_to_add = """
      [ { :db/id #db/id[:db.part/db]
          :db/ident :person/name
          :db/valueType :db.type/string
          :db/cardinality :db.cardinality/one
          :db/doc \"A person's name\"
          :db.install/_attribute :db.part/db}]
    """
    {:ok, _} = DatomicGenServer.transact(DatomicGenServer, data_to_add)

    query = "[:find ?e :in $ ?idmin :where [?e :db/ident :person/name][(> ?e ?idmin)]]"
    {:error, result_str} = DatomicGenServer.q(DatomicGenServer, query, 
                            ["datomic_gen_server.peer/*db*", "\"(- 1 1)\""])
                            
    assert Regex.match?(~r/java.lang.Long cannot be cast to java.lang.String/, result_str)
  end
  
  test "can issue as-of queries" do
    data_to_add = [%{ 
        Db.id => Db.dbid(Db.schema_partition),
        Db.ident => :"person/address",
        Db.value_type => Db.type_string,
        Db.cardinality => Db.cardinality_one,
        Db.doc => "A person's address",
        Db.install_attribute => Db.schema_partition
    }]
    
    # Get an interpreted struct so we can use the tx time.
    {:ok, transaction_result} = Db.transact(DatomicGenServer, data_to_add)
    
    query = "[:find ?ident :in $ ?docstring :where [?e :db/doc ?docstring][?e :db/ident ?ident]]"
    
    before_result = DatomicGenServer.q(DatomicGenServer, query, 
      ["(datomic.api/as-of datomic_gen_server.peer/*db* #{transaction_result.basis_t_before})", "\"A person's address\""]
    ) 
    assert {:ok, "\#{}\n"} == before_result
    
    after_result = DatomicGenServer.q(DatomicGenServer, query, 
      ["(datomic.api/as-of datomic_gen_server.peer/*db* #{transaction_result.basis_t_after})", "\"A person's address\""]
    ) 
    assert {:ok, "\#{[:person/address]}\n"} == after_result
  end
  
  test "can handle multiple messages from different processes" do
    Enum.each(1..10, (fn(index) -> spawn (fn () -> 
      data_to_add = """
        [ { :db/id #db/id[:db.part/db]
            :db/ident :some/field#{index}
            :db/valueType :db.type/string
            :db/cardinality :db.cardinality/one
            :db/doc \"Field #{index}\"
            :db.install/_attribute :db.part/db}]
      """
      :timer.sleep(:random.uniform(3)) # Mix up the order of sending the messages
      {:ok, transaction_result} = DatomicGenServer.transact(DatomicGenServer, data_to_add)
      assert Regex.match?(~r/:db-before \{:basis-t \d+/, transaction_result)
      assert Regex.match?(~r/Field #{index}/, transaction_result)
    end) end))
  end
  
  test "can pull an entity" do
    data_to_add = """
      [ {:db/id #db/id[:db.part/db]
         :db/ident :person/city
         :db/valueType :db.type/string
         :db/cardinality :db.cardinality/one
         :db/doc \"A person's city\"
         :db.install/_attribute :db.part/db} ]
    """
    {:ok, _} = DatomicGenServer.transact(DatomicGenServer, data_to_add)
    
    {:ok, entity_id_result} = DatomicGenServer.q(DatomicGenServer, "[:find ?e :where [?e :db/ident :person/city]]")
    assert Regex.match?(~r/\#\{\[\d+\]\}/, entity_id_result)
    
    entity_id = Regex.replace(~r/\#\{\[(\d+)\]\}/, entity_id_result, "\\1")

    {:ok, pull_result} = DatomicGenServer.pull(DatomicGenServer, "[*]", "#{entity_id}")
    assert Regex.match?(~r/:db\/ident :person\/city/, pull_result)
    assert Regex.match?(~r/:db\/doc "A person's city"/, pull_result)
  end
  
  test "can pull many entities" do
    data_to_add = """
      [ {:db/id #db/id[:db.part/db]
         :db/ident :person/state
         :db/valueType :db.type/string
         :db/cardinality :db.cardinality/one
         :db/doc \"A person's state\"
         :db.install/_attribute :db.part/db} ]
    """
    {:ok, _} = DatomicGenServer.transact(DatomicGenServer, data_to_add)
    {:ok, entity_id_result} = DatomicGenServer.q(DatomicGenServer, "[:find ?e :where [?e :db/ident :person/state]]")
    entity_id_1 = Regex.replace(~r/\#\{\[(\d+)\]\}/, entity_id_result, "\\1")
    
    data_to_add = """
      [ {:db/id #db/id[:db.part/db]
         :db/ident :person/zip
         :db/valueType :db.type/string
         :db/cardinality :db.cardinality/one
         :db/doc \"A person's zip code\"
         :db.install/_attribute :db.part/db} ]
    """
    {:ok, _} = DatomicGenServer.transact(DatomicGenServer, data_to_add)

    {:ok, pull_result} = DatomicGenServer.pull_many(DatomicGenServer, "[*]", "[#{entity_id_1} :person/zip]")
    assert Regex.match?(~r/:db\/ident :person\/state/, pull_result)
    assert Regex.match?(~r/:db\/doc "A person's state"/, pull_result)
    assert Regex.match?(~r/:db\/ident :person\/zip/, pull_result)
    assert Regex.match?(~r/:db\/doc "A person's zip code"/, pull_result)
  end

  test "can ask for an entity" do
    data_to_add = """
      [ {:db/id #db/id[:db.part/db]
         :db/ident :person/email
         :db/valueType :db.type/string
         :db/cardinality :db.cardinality/one
         :db/doc \"A person's email\"
         :db.install/_attribute :db.part/db} ]
    """
    {:ok, _} = DatomicGenServer.transact(DatomicGenServer, data_to_add)

    all_attributes = "{:db/ident :person/email, :db/valueType :db.type/string, :db/cardinality :db.cardinality/one, :db/doc \"A person's email\"}\n"
    {:ok, entity_result} = DatomicGenServer.entity(DatomicGenServer, ":person/email")
    assert all_attributes == entity_result

    {:ok, entity_result2} = DatomicGenServer.entity(DatomicGenServer, ":person/email", :all)
    assert all_attributes == entity_result2

    {:ok, entity_result3} = DatomicGenServer.entity(DatomicGenServer, ":person/email", [:"db/valueType", :"db/doc"])
    assert "{:db/valueType :db.type/string, :db/doc \"A person's email\"}\n" == entity_result3
     
    {:ok, entity_result4} = DatomicGenServer.entity(DatomicGenServer, "[:db/ident :person/email]", [:"db/cardinality"])
    assert "{:db/cardinality :db.cardinality/one}\n" == entity_result4
  end
  
  test "Handles garbled queries" do
    query = "[:find ?c :wh?ere]"
    {:error, query_result} = DatomicGenServer.q(DatomicGenServer, query)
    assert Regex.match?(~r/Exception/, query_result)
  end
  
  test "Handles garbled transactions" do
    data_to_add = """
      [ { :db/ii #db/foo[:db.part/db]
    """
    {:error, transaction_result} = DatomicGenServer.transact(DatomicGenServer, data_to_add)
    assert Regex.match?(~r/Exception/, transaction_result)
  end
end
