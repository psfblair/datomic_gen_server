defmodule DatomicGenServer do
  use GenServer
  require Logger
  @moduledoc """
    DatomicGenServer is an Elixir GenServer that communicates with a Clojure 
    Datomic peer running in the JVM, using clojure-erlastic.

    The interface functions in this module communicate with Datomic using edn
    strings. To use Elixir data structures, see the accompanying `DatomicGenServer.db`
    module.

## Examples
    
      DatomicGenServer.start(
        "datomic:mem://test", 
        true, 
        [{:timeout, 20_000}, {:default_message_timeout, 20_000}, {:name, DatomicGenServer}]
      )
      
      query = "[:find ?c :where [?c :db/doc \\"Some docstring that isn't in the database\\"]]"
      DatomicGenServer.q(DatomicGenServer, query)
      
      # => {:ok, "\#{}\\n"}
      
      data_to_add = \"\"\"
        [ { :db/id #db/id[:db.part/db]
            :db/ident :person/name
            :db/valueType :db.type/string
            :db/cardinality :db.cardinality/one
            :db/doc \\"A person's name\\"
            :db.install/_attribute :db.part/db}]
      \"\"\"
      
      DatomicGenServer.transact(DatomicGenServer, data_to_add)

      # => {:ok, "{:db-before {:basis-t 1000}, :db-after {:basis-t 1000}, 
                    :tx-data [{:a 50, :e 13194139534313, :v #inst \\"2016-02-14T02:10:54.580-00:00\\", 
                    :tx 13194139534313, :added true} {:a 10, :e 64, :v :person/name, :tx 13194139534313, 
                    :added true} {:a 40, :e 64, :v 23, :tx 13194139534313, :added true} {:a 41, 
                    :e 64, :v 35, :tx 13194139534313, :added true} {:a 62, :e 64, 
                    :v \\"A person's name\\", :tx 13194139534313, :added true} {:a 13, 
                    :e 0, :v 64, :tx 13194139534313, :added true}], :tempids {-9223367638809264705 64}}"}
  """
  @type datomic_message :: {:q, integer, String.t, [String.t]} | 
                           {:transact, integer, String.t} | 
                           {:entity, integer, String.t, [atom] | :all} |
                           {:migrate, integer, String.t} |
                           {:load, integer, String.t}
  @type datomic_call :: {datomic_message, message_timeout :: non_neg_integer}
  @type datomic_result :: {:ok, String.t} | {:error, term}
  @type start_option :: GenServer.option | {:default_message_timeout, non_neg_integer}
  @type send_option :: {:message_timeout, non_neg_integer} | {:client_timeout, non_neg_integer}

  # These should be overridden either by application configs or by passed parameters
  @last_ditch_startup_timeout 5000
  @last_ditch_default_message_timeout 5000
  
  defmodule ProcessState do
    defstruct port: nil, message_wait_until_crash: 5_000
    @type t :: %ProcessState{port: port, message_wait_until_crash: non_neg_integer}
  end
  
############################# INTERFACE FUNCTIONS  ############################
  @doc """
  Starts the GenServer. 
  
  This function is basically a pass-through to `GenServer.start`, but with some 
  additional parameters: The first is the URL of the Datomic transactor to which 
  to connect, and the second a boolean parameter indicating whether or not to 
  create the database if it does not yet exist. 
  
  The options keyword list may include the normal options accepted by `GenServer.start`, 
  as well as a `:default_message_timeout` option that controls the default time in 
  milliseconds that the server will wait for a message before crashing. Note that
  if the `:timeout` option is provided, the GenServer will crash if that timeout
  is exceeded.
  
## Example
  
      DatomicGenServer.start(
        "datomic:mem://test", 
        true, 
        [{:timeout, 20_000}, {:default_message_timeout, 20_000}, {:name, DatomicGenServer}]
      )

  """
  @spec start(String.t, boolean, [start_option]) :: GenServer.on_start
  def start(db_uri, create? \\ false, options \\ []) do
    {params, options_without_name} = startup_params(db_uri, create?, options)
    GenServer.start(__MODULE__, params, options_without_name)  
  end

  @doc """
  Starts the GenServer in a linked process. 
  
  This function is basically a pass-through to `GenServer.start_link`, but with 
  some additional parameters: The first is the URL of the Datomic transactor to 
  which to connect, and the second a boolean parameter indicating whether or not 
  to create the database if it does not yet exist. 
  
  The options keyword list may include the normal options accepted by `GenServer.start_link`, 
  as well as a `:default_message_timeout` option that controls the default time in 
  milliseconds that the server will wait for a message before crashing. Note that
  if the `:timeout` option is provided, the GenServer will crash if that timeout
  is exceeded.
  
## Example
  
      DatomicGenServer.start_link(
        "datomic:mem://test", 
        true, 
        [{:timeout, 20_000}, {:default_message_timeout, 20_000}, {:name, DatomicGenServer}]
      )

  """
  @spec start_link(String.t, boolean, [start_option]) :: GenServer.on_start
  def start_link(db_uri, create? \\ false, options \\ []) do
    {params, options_without_name} = startup_params(db_uri, create?, options)
    GenServer.start_link(__MODULE__, params, options_without_name)  
  end
  
  defp startup_params(db_uri, create?, options) do
    {startup_wait, options} = Keyword.pop(options, :timeout) 
    startup_wait = startup_wait || Application.get_env(:datomic_gen_server, :startup_wait_millis) || @last_ditch_startup_timeout
    
    {default_message_timeout, options} = Keyword.pop(options, :default_message_timeout) 
    default_message_timeout = default_message_timeout || Application.get_env(:datomic_gen_server, :message_wait_until_crash) || @last_ditch_default_message_timeout
    
    {maybe_process_identifier, options} = Keyword.pop(options, :name) 
    
    params = {db_uri, create?, maybe_process_identifier, startup_wait, default_message_timeout}
    {params, options}
  end

  @doc """
  Queries a DatomicGenServer using a query formulated as an edn string.
  This query is passed to the Datomic `q` API function.
  
  The first parameter to this function is the pid or alias of the GenServer process;
  the second is the query. The options keyword list may include a `:client_timeout` 
  option that specifies the milliseconds timeout passed to GenServer.call, and a 
  `:message_timeout` option that specifies how long the GenServer should wait
  for a response before crashing (overriding the default value set in `start` or
  `start_link`). Note that if the `:client_timeout` is shorter than the 
  `:message_timeout` value, the call will return an error but the server will
  not crash even if the response message is never returned from the Clojure peer.
  
## Example
  
      query = "[:find ?c :where [?c :db/doc \\"Some docstring that isn't in the database\\"]]"
      DatomicGenServer.q(DatomicGenServer, query)
      
      => {:ok, "\#{}\\n"}
      
  """
  @spec q(GenServer.server, String.t, [String.t], [send_option]) :: datomic_result
  def q(server_identifier, edn_str, bindings_edn \\ [], options \\ []) do
    # Note that clojure-erltastic sends an empty list to Clojure as nil. This 
    # interferes with wait_for_response - if an error comes back it expects to
    # find the original message, but Clojure will return the original message as
    # having a nil where we are waiting for the one we sent, which contains an
    # empty list. To protect against this situation we never send empty lists to
    # Clojure; only nil.
    bindings = if bindings_edn && ! Enum.empty?(bindings_edn) do bindings_edn else nil end
    msg_unique_id = :erlang.unique_integer([:monotonic])
    call_server(server_identifier, {:q, msg_unique_id, edn_str, bindings}, options)
  end
  
  @doc """
  Issues a transaction against a DatomicGenServer using a transaction 
  formulated as an edn string. This transaction is passed to the Datomic `transact` 
  API function. 
  
  The first parameter to this function is the pid or alias of the GenServer process; 
  the second is the transaction data in edn format. The options keyword list may 
  include a `:client_timeout` option that specifies the milliseconds timeout passed 
  to GenServer.call, and a `:message_timeout` option that specifies how long the 
  GenServer should wait for a response before crashing (overriding the default 
  value set in `start` or `start_link`). Note that if the `:client_timeout` is 
  shorter than the `:message_timeout` value, the call will return an error but 
  the server will not crash even if the response message is never returned from 
  the Clojure peer.
  
## Example
  
      data_to_add = \"\"\"
        [ { :db/id #db/id[:db.part/db]
            :db/ident :person/name
            :db/valueType :db.type/string
            :db/cardinality :db.cardinality/one
            :db/doc \\"A person's name\\"
            :db.install/_attribute :db.part/db}]
      \"\"\"
      
      DatomicGenServer.transact(DatomicGenServer, data_to_add)
      
      => {:ok, "{:db-before {:basis-t 1000}, :db-after {:basis-t 1000}, 
                :tx-data [{:a 50, :e 13194139534313, :v #inst \\"2016-02-14T02:10:54.580-00:00\\", 
                :tx 13194139534313, :added true} {:a 10, :e 64, :v :person/name, :tx 13194139534313, 
                :added true} {:a 40, :e 64, :v 23, :tx 13194139534313, :added true} {:a 41, 
                :e 64, :v 35, :tx 13194139534313, :added true} {:a 62, :e 64, 
                :v \\"A person's name\\", :tx 13194139534313, :added true} {:a 13, 
                :e 0, :v 64, :tx 13194139534313, :added true}], :tempids {-9223367638809264705 64}}"}
  
  """
  @spec transact(GenServer.server, String.t, [send_option]) :: datomic_result
  def transact(server_identifier, edn_str, options \\ []) do
    msg_unique_id = :erlang.unique_integer([:monotonic])
    call_server(server_identifier, {:transact, msg_unique_id, edn_str}, options)
  end
  
  @doc """
  Issues an `entity` call that is passed to the Datomic `entity` API function. 
  
  The first parameter to this function is the pid or alias of the GenServer process; 
  the second is an edn string representing the parameter that is to be passed to 
  `entity`: either an entity id, an ident, or a lookup ref. The third parameter 
  is a list of atoms that represent the keys of the attributes you wish to fetch, 
  or `:all` if you want all the entity's attributes. 
  
  The options keyword list may include a `:client_timeout` option that specifies 
  the milliseconds timeout passed to GenServer.call, and a `:message_timeout` 
  option that specifies how long the GenServer should wait for a response before 
  crashing (overriding the default value set in `start` or `start_link`). Note 
  that if the `:client_timeout` is shorter than the `:message_timeout` value, 
  the call will return an error but the server will not crash even if the message 
  is never returned from the Clojure peer.
  
## Example
  
      DatomicGenServer.entity(DatomicGenServer, ":person/email", [:"db/valueType", :"db/doc"])
      
      => {:ok, "{:db/valueType :db.type/string, :db/doc \\"A person's email\\"}\\n"}
      
  """
  @spec entity(GenServer.server, String.t, [atom] | :all, [send_option]) :: datomic_result
  def entity(server_identifier, edn_str, attr_names \\ :all, options \\ []) do
    msg_unique_id = :erlang.unique_integer([:monotonic])
    call_server(server_identifier, {:entity, msg_unique_id, edn_str, attr_names}, options)
  end

  @doc """
  Issues a call to net.phobot.datomic/migrator to migrate a database using
  database migration files in edn format.
  
  The first parameter to this function is the pid or alias of the GenServer process; 
  the second is the path to the directory containing the migration files. Files
  will be processed in sort order. The Clojure Conformity library is used to keep
  the migrations idempotent. 
  
  The options keyword list may include a `:client_timeout` option that specifies 
  the milliseconds timeout passed to GenServer.call, and a `:message_timeout` 
  option that specifies how long the GenServer should wait for a response before 
  crashing (overriding the default value set in `start` or `start_link`). Note 
  that if the `:client_timeout` is shorter than the `:message_timeout` value, 
  the call will return an error but the server will not crash even if the message 
  is never returned from the Clojure peer.
  
## Example
  
      DatomicGenServer.migrate(DatomicGenServer, Path.join [System.cwd(), "migrations"])
      
      => {:ok, :migrated}
      
  """
  @spec migrate(GenServer.server, String.t, [send_option]) :: datomic_result
  def migrate(server_identifier, migration_path, options \\ []) do
    msg_unique_id = :erlang.unique_integer([:monotonic])
    call_server(server_identifier, {:migrate, msg_unique_id, migration_path}, options)
  end

  @doc """
  Issues a call to the Clojure net.phobot.datomic/seed library to load data into 
  a database using data files in edn format. The database is not dropped, 
  recreated, or migrated before loading.
  
  The first parameter to this function is the pid or alias of the GenServer process; 
  the second is the path to the directory containing the data files. The data 
  files will be processed in the sort order of their directory. 
  
  Data is loaded in a single transaction. The return value of the function 
  is the result of the Datomic `transact` API function call that executed the
  transaction. If you want this result in a struct, call the wrapper `load` 
  function in the `DatomicGenServer.Db` module.
  
  Loading data does not use the Clojure Conformity library and is not idempotent.
  
  The options keyword list may include a `:client_timeout` option that specifies 
  the milliseconds timeout passed to GenServer.call, and a `:message_timeout` 
  option that specifies how long the GenServer should wait for a response before 
  crashing (overriding the default value set in `start` or `start_link`). Note 
  that if the `:client_timeout` is shorter than the `:message_timeout` value, 
  the call will return an error but the server will not crash even if the message 
  is never returned from the Clojure peer.
  
## Example
      data_dir = Path.join [System.cwd(), "seed-data"]
      DatomicGenServer.load(DatomicGenServer, data_dir)
      
      => {:ok, "{:db-before {:basis-t 1000}, :db-after {:basis-t 1000}, ...
      
  """
  @spec load(GenServer.server, String.t, [send_option]) :: datomic_result
  def load(server_identifier, data_path, options \\ []) do
    msg_unique_id = :erlang.unique_integer([:monotonic])
    call_server(server_identifier, {:load, msg_unique_id, data_path}, options)
  end
  
  @spec call_server(GenServer.server, datomic_message, [send_option]) :: datomic_result
  defp call_server(server_identifier, request, options) do
    {message_timeout, client_timeout} = message_wait_times(options)
    if client_timeout do
      GenServer.call(server_identifier, {request, message_timeout}, client_timeout)
    else
      GenServer.call(server_identifier, {request, message_timeout})
    end
  end
  
  defp message_wait_times(options) do
    # If it's nil, this value value is nil and we'll use the general default when handling the call.
    message_timeout = Keyword.get(options, :message_timeout) 
    
    client_timeout = Keyword.get(options, :client_timeout) 
    client_timeout = client_timeout || Application.get_env(:datomic_gen_server, :timeout_on_call)

    {message_timeout, client_timeout}
  end
  
############################# CALLBACK FUNCTIONS  ##############################

  # Implements the GenServer `init` callback; clients should not call this function. 
  # 
  # On start, the server sends itself an initial message to start the JVM, then 
  # registers itself under any alias provided. Any messages sent to the server by 
  # clients at startup will arrive after the initialization message, and will need 
  # to wait until the JVM starts and initialization is complete. Thus, it is important 
  # that the timeouts on messages sent to the server exceed the startup timeout value, 
  # at least for the messages sent during the startup phase.
  @spec init({String.t, boolean, GenServer.name | nil, non_neg_integer, non_neg_integer}) :: {:ok, ProcessState.t}
  def init({db_uri, create?, maybe_process_identifier, startup_wait_millis, default_message_timeout_millis}) do
    # Trapping exits actually does what we want here - i.e., allows us to exit
    # if the Clojure process crashes with an error on startup, using handle_info below.
    Process.flag(:trap_exit, true)
    
    send(self, {:initialize_jvm, db_uri, create?, startup_wait_millis})
    if maybe_process_identifier do
      Process.register(self, maybe_process_identifier)
    end
    {:ok, %ProcessState{port: nil, message_wait_until_crash: default_message_timeout_millis}}
  end
  
  defp start_jvm_command(db_uri, create?) do
    create_str = if create?, do: "true", else: ""
    working_directory = "#{:code.priv_dir(:datomic_gen_server)}/datomic_gen_server_peer"
    command = "java -cp target/peer*standalone.jar datomic_gen_server.peer #{db_uri} #{create_str}"
    {working_directory, command}
  end
  
  defp my_name do
    Process.info(self) |> Keyword.get(:registered_name) || self |> inspect
  end

  # Implements the GenServer `handle_info` callback for the initial message that 
  # starts the JVM. This is an internal message used for initialization; clients should not send this message to the GenServer.
  @spec handle_info({:initialize_jvm, String.t, boolean, non_neg_integer}, ProcessState.t) :: {:noreply, ProcessState.t}
  def handle_info({:initialize_jvm, db_uri, create?, startup_wait_millis}, state) do
    {working_directory, command} = start_jvm_command(db_uri, create?)
    port = Port.open({:spawn, '#{command}'}, [:binary, :exit_status, packet: 4, cd: working_directory])

    # Block until JVM starts up, or we're not ready
    send(port, {self, {:command, :erlang.term_to_binary({:ping})}})
    receive do
      # Make sure we're only listening for a message back from the port, not some
      # message from a caller that may have gotten in first.
      {^port, {:data, _}} -> {:noreply, %ProcessState{port: port, message_wait_until_crash: state.message_wait_until_crash}}
      {:EXIT, _, _} ->
        _ = Logger.error("DatomicGenServer #{my_name} port exited with error on startup.")
        exit(:port_exited_with_error)
    after startup_wait_millis -> 
      _ = Logger.error("DatomicGenServer #{my_name} port startup timed out after startup_wait_millis: [#{startup_wait_millis}]")
      exit(:port_start_timed_out)
    end
  end
  
  # Handle exit messages
  @spec handle_info({:EXIT, port, term}, ProcessState.t) :: no_return
  def handle_info({:EXIT, _, _}, _) do
    _ = Logger.warn("DatomicGenServer #{my_name} received exit message.")
    exit(:port_terminated)
  end

  # Do-nothing implementation of the GenServer `handle_info` callback as a catch-all case. 
  # Not sure how to do spec for this catch-all case without Dialyzer telling me
  # I have overlapping domains.
  def handle_info(_, state) do
   {:noreply, state}
  end

 # Implements the GenServer `handle_call` callback to handle client messages. 
 # 
 # This function sends a message to the Clojure peer that is run on a port, and waits 
 # for a response from the peer with the same message ID. Messages that are returned
 # from the port with different message IDs (for example, responses to earlier
 # requests that timed out) are discarded. 
  @spec handle_call(datomic_call, term, ProcessState.t) :: {:reply, datomic_result, ProcessState.t}
  def handle_call(message, _, state) do
    port = state.port
    {datomic_operation, this_msg_timeout} = message
    message_timeout = this_msg_timeout || state.message_wait_until_crash
    send(port, {self, {:command, :erlang.term_to_binary(datomic_operation)}})
    response = wait_for_reply(port, datomic_operation, message_timeout, this_msg_timeout, state.message_wait_until_crash)
    {:reply, response, state}
  end
  
  # Need this in case an earlier call times out without the GenServer crashing, and
  # then the reply for that call from the Clojure port comes back before the 
  # response to the message we're waiting for. Right now if there is such a 
  # message we just clear it out of the mailbox and keep going. In the future, 
  # if we're handling async messages we will need to do something more intelligent.
  # It would be nice if we could just use a pattern match in the receive clause
  # for this purpose, but we need to call :erlang.binary_to_term on the message
  # to see what's in it, and we can't do that without handling the message. 
  defp wait_for_reply(port, sent_message, message_timeout, this_msg_timeout, message_wait_until_crash) do
    start_time = :os.system_time(:milli_seconds)
    
    response = receive do 
      {^port, {:data, b}} -> :erlang.binary_to_term(b) 
    after message_timeout -> 
      _ = Logger.error("DatomicGenServer #{my_name} port unresponsive with this_msg_timeout #{this_msg_timeout} and message_wait_until_crash #{message_wait_until_crash}")
      exit(:port_unresponsive)
    end
    
    message_unique_id = if is_tuple(sent_message) && tuple_size(sent_message) > 1 do
      elem(sent_message, 1)
    else
      nil
    end
    
    elapsed = :os.system_time(:milli_seconds) - start_time
    
    # Determine if this is a response to the message we were waiting for. If not, recurse
    case response do
      {:ok, response_id, reply} when message_unique_id == response_id -> {:ok, reply}
      {:error, ^sent_message, error} -> {:error, error}
      _ -> wait_for_reply(port, message_unique_id, (message_timeout - elapsed), this_msg_timeout, message_wait_until_crash)
    end
  end

  # Implements the GenServer `handle_cast` callback to handle client messages. 
  # 
  # This function sends a message to the Clojure peer that is run on a port, but does
  # not wait for the result. Responses from the peer will be discarded either in 
  # the `wait_for_reply` loop called by `handle_call`, or in the `handle_info` 
  # do-nothing catch-all function.
  @spec handle_cast(datomic_message, ProcessState.t) :: {:noreply, ProcessState.t}
  def handle_cast(message, state) do
    port = state.port
    send(port, {self, {:command, :erlang.term_to_binary(message)}})
    {:noreply, state}
  end
end
