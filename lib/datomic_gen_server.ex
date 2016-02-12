defmodule DatomicGenServer do
  use GenServer
  require Logger

  @type datomic_message :: {:q, String.t} | {:transact, String.t}
  @type datomic_call :: {datomic_message, message_timeout :: non_neg_integer}
  @type datomic_result :: {:ok, String.t} | {:error, term}
  @type start_option :: GenServer.option | {:default_message_timeout, non_neg_integer}

  # These should be overridden either by application configs or by passed parameters
  @last_ditch_startup_timeout 5000
  @last_ditch_default_message_timeout 5000
  
  defmodule ProcessState do
    defstruct port: nil, message_wait_until_crash: 5_000
    @type t :: %ProcessState{port: port, message_wait_until_crash: non_neg_integer}
  end
  
  @spec start(String.t, boolean, [start_option]) :: GenServer.on_start
  def start(db_uri, create? \\ false, options \\ []) do
    {params, options} = startup_params(db_uri, create?, options)
    GenServer.start(__MODULE__, params, options)  
  end

  @spec start_link(String.t, boolean, [start_option]) :: GenServer.on_start
  def start_link(db_uri, create? \\ false, options \\ []) do
    {params, options} = startup_params(db_uri, create?, options)
    GenServer.start_link(__MODULE__, params, options)  
  end
  
  defp startup_params(db_uri, create?, options) do
    {startup_wait, options} = Keyword.pop(options, :timeout) 
    startup_wait = startup_wait || Application.get_env(:datomic_gen_server, :startup_wait_millis) || @last_ditch_startup_timeout
    
    {default_message_timeout, options} = Keyword.pop(options, :default_message_timeout) 
    default_message_timeout = default_message_timeout || Application.get_env(:datomic_gen_server, :message_wait_until_crash) || @last_ditch_default_message_timeout
    
    params = {db_uri, create?, startup_wait, default_message_timeout}
    {params, Keyword.put(options, :name, __MODULE__)}
  end

  # TODO Add timeouts as an option map.
  @spec q(String.t, non_neg_integer | nil, non_neg_integer | nil) :: datomic_result
  def q(edn_str, message_timeout_millis \\ nil, timeout_on_call \\ nil) do
    {message_timeout, client_timeout} = message_wait_times(message_timeout_millis, timeout_on_call)
    GenServer.call(__MODULE__, {{:q, edn_str}, message_timeout}, client_timeout)
  end
  
  @spec transact(String.t, non_neg_integer | nil, non_neg_integer | nil) :: datomic_result
  def transact(edn_str, message_timeout_millis \\ nil, timeout_on_call \\ nil) do
    {message_timeout, client_timeout} = message_wait_times(message_timeout_millis, timeout_on_call)
    GenServer.call(__MODULE__, {{:transact, edn_str}, message_timeout}, client_timeout)
  end
  
  @spec entity(String.t, [atom] | :all, non_neg_integer | nil, non_neg_integer | nil) :: datomic_result
  def entity(edn_str, attr_names \\ :all, message_timeout_millis \\ nil, timeout_on_call \\ nil) do
    {message_timeout, client_timeout} = message_wait_times(message_timeout_millis, timeout_on_call)
    GenServer.call(__MODULE__, {{:entity, edn_str, attr_names}, message_timeout}, client_timeout)
  end

  @spec exit :: {:stop, :normal}
  def exit() do
    _ = Logger.warn("exit called on DatomicGenServer.")
    {:stop, :normal}
  end
  
  defp message_wait_times(message_timeout_millis, call_timeout_millis) do
    # If it's nil, this value is nil and we'll use the general default when handling the call.
    message_timeout = if is_nil(message_timeout_millis) do nil else message_timeout_millis end
    call_timeout = call_timeout_millis || Application.get_env(:datomic_gen_server, :timeout_on_call)
    {message_timeout, call_timeout}
  end

  # TODO Return from init faster by sending a message that is handled in handle_info 
  # to do the initialization, then register after sending the info message.
  @spec init({String.t, boolean, non_neg_integer, non_neg_integer}) :: {:ok, ProcessState.t}
  def init({db_uri, create?, startup_wait_millis, default_message_timeout_millis}) do
    # Trapping exits actually does what we want here - i.e., allows us to exit
    # if the Clojure process crashes on startup, using handle_info below.
    Process.flag(:trap_exit, true)
    {working_directory, command} = start_jvm_command(db_uri, create?)
    port = Port.open({:spawn, '#{command}'}, [:binary, packet: 4, cd: working_directory])

    # Block until JVM starts up, or we're not ready
    send(port, {self, {:command, :erlang.term_to_binary({:ping})}})
    receive do
      _ -> {:ok, %ProcessState{port: port, message_wait_until_crash: default_message_timeout_millis}}
    after startup_wait_millis -> 
      _ = Logger.error("DatomicGenServer port startup timed out after startup_wait_millis: [#{startup_wait_millis}]")
      exit(:port_start_timed_out)
    end
  end
  
  defp start_jvm_command(db_uri, create?) do
    create_str = if create?, do: "true", else: ""
    working_directory = "#{:code.priv_dir(:datomic_gen_server)}/datomic_gen_server_peer"
    command = "java -cp target/peer*standalone.jar datomic_gen_server.peer #{db_uri} #{create_str}"
    {working_directory, command}
  end
  
  @spec handle_call(datomic_call, term, ProcessState.t) :: {:reply, datomic_result, ProcessState.t}
  def handle_call(message, _, state) do
    port = state.port
    {datomic_operation, this_msg_timeout} = message
    message_timeout = this_msg_timeout || state.message_wait_until_crash
    
    send(port, {self, {:command, :erlang.term_to_binary(datomic_operation)}})
    
    result = receive do 
      {^port, {:data, b}} -> :erlang.binary_to_term(b) 
    after message_timeout -> 
      _ = Logger.error("DatomicGenServer port unresponsive after message_timeout: [#{message_timeout}] with this_msg_timeout [#{this_msg_timeout}] and message_wait_until_crash [#{state.message_wait_until_crash}]")
      exit(:port_unresponsive)
    end

    {:reply, result, state}
  end
  
  @spec handle_cast(datomic_message, ProcessState.t) :: {:noreply, ProcessState.t}
  def handle_cast(message, state) do
    port = state.port
    send(port, {self, {:command, :erlang.term_to_binary(message)}})
    {:noreply, state}
  end

  # TODO Indicate which gen server it is.
  @spec handle_info({:EXIT, port, term}, ProcessState.t) :: no_return
  def handle_info({:EXIT, _, _}, _) do
    _ = Logger.warn("DatomicGenServer received exit message.")  
    exit(:port_terminated)
  end
  
  # Not sure how to do spec for this catch-all case without Dialyzer telling me
  # I have overlapping domains.
  def handle_info(_, state), do: {:noreply, state}
  
  @spec terminate(reason :: term, state :: ProcessState.t) :: true
  def terminate(reason, _) do
    # Normal shutdown; die ASAP
    Process.exit(self, reason)
  end
end
