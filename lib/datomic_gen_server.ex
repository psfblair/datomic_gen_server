defmodule DatomicGenServer do
  use GenServer
  require Logger

  @type datomic_message :: {:q, String.t} | {:transact, String.t} | {:entity, String.t, [atom]}
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
    {params, options}
  end

  @spec q(GenServer.server, String.t, [send_option]) :: datomic_result
  def q(server_identifier, edn_str, options \\ []) do
    call_server(server_identifier, {:q, edn_str}, options)
  end
  
  @spec transact(GenServer.server, String.t, [send_option]) :: datomic_result
  def transact(server_identifier, edn_str, options \\ []) do
    call_server(server_identifier, {:transact, edn_str}, options)
  end
  
  @spec entity(GenServer.server, String.t, [atom] | :all, [send_option]) :: datomic_result
  def entity(server_identifier, edn_str, attr_names \\ :all, options \\ []) do
    call_server(server_identifier, {:entity, edn_str, attr_names}, options)
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
  
  @spec exit :: {:stop, :normal}
  def exit() do
    _ = Logger.warn("exit called on DatomicGenServer.")
    {:stop, :normal}
  end
  
############################# CALLBACK FUNCTIONS  ##############################
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
