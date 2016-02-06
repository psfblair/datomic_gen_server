defmodule DatomicGenServer do
  use GenServer
  
  @type datomic_message :: {:q, String.t} | {:transact, String.t} | {:exit}
  @type datomic_result :: {:ok, String.t} | {:error, term}

  defmodule ProcessState do
    defstruct port: nil, 
              message_wait_until_crash: 5_000
    @type t :: %ProcessState{port: port, message_wait_until_crash: non_neg_integer}
  end
  
  @spec start_link(String.t, boolean, non_neg_integer, non_neg_integer) :: GenServer.on_start
  def start_link(db_uri, create? \\ false, startup_wait_millis \\ nil, default_message_timeout_millis \\ nil) do
    startup_wait = startup_wait_millis || Application.get_env(:datomic_gen_server, :startup_wait_millis)
    default_message_timeout = default_message_timeout_millis || Application.get_env(:datomic_gen_server, :message_wait_until_crash)
  
    params = {db_uri, create?, startup_wait, default_message_timeout}
    GenServer.start_link(__MODULE__, params, name: __MODULE__)  
  end

  @type edn :: atom | boolean | number | String.t | tuple | [edn] | %{edn => edn} | MapSet.t(edn) 
  @spec q([term], non_neg_integer, non_neg_integer) :: datomic_result
  def q(edn, message_timeout_millis \\ nil, timeout_on_call \\ nil) do
    {message_timeout, client_timeout} = message_wait_times(message_timeout_millis, timeout_on_call)
    case Exdn.from_elixir(edn) do
      {:ok, edn_str} -> GenServer.call(__MODULE__, {{:q, edn_str}, message_timeout}, client_timeout)
      parse_error -> parse_error
    end
  end
  
  @spec transact([term], non_neg_integer, non_neg_integer) :: datomic_result
  def transact(edn, message_timeout_millis \\ nil, timeout_on_call \\ nil) do
    {message_timeout, client_timeout} = message_wait_times(message_timeout_millis, timeout_on_call)
    case Exdn.from_elixir(edn) do
      {:ok, edn_str} -> GenServer.call(__MODULE__, {{:transact, edn_str}, message_timeout}, client_timeout)
      parse_error -> parse_error
    end
  end

  @spec exit :: :ok
  def exit() do
    GenServer.cast(__MODULE__, {:exit})
  end
  
  defp message_wait_times(message_timeout_millis, call_timeout_millis) do
    # If it's nil, this value is nil and we'll use the general default when handling the call.
    message_timeout = if is_nil(message_timeout_millis) do nil else message_timeout_millis end
    call_timeout = call_timeout_millis || Application.get_env(:datomic_gen_server, :timeout_on_call)
    {message_timeout, call_timeout}
  end

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
      {:stop, :port_start_timed_out}
    end
  end
  
  defp start_jvm_command(db_uri, create?) do
    create_str = if create?, do: "true", else: ""
    working_directory = "#{:code.priv_dir(:datomic_gen_server)}/datomic_gen_server_peer"
    command = "java -cp target/peer*standalone.jar datomic_gen_server.peer #{db_uri} #{create_str}"
    {working_directory, command}
  end
  
  @spec handle_call(datomic_message, {pid, tag :: term}, ProcessState.t) :: {:reply, datomic_result, ProcessState.t}
  def handle_call(term, _, state) do
    port = state.port
    {datomic_operation, this_msg_timeout} = term
    message_timeout = this_msg_timeout || state.message_wait_until_crash
    
    send(port, {self, {:command, :erlang.term_to_binary(datomic_operation)}})
    
    result = receive do 
      {^port, {:data, b}} -> :erlang.binary_to_term(b) 
    after message_timeout -> 
      exit(:port_unresponsive)
    end

    {:reply, result, state}
  end
  
  @spec handle_cast(datomic_message, ProcessState.t) :: {:noreply, ProcessState.t}
  def handle_cast(term, state) do
    port = state.port
    send(port, {self, {:command, :erlang.term_to_binary(term)}})
    {:noreply, state}
  end

  # TODO Handle other info cases?
  @spec handle_info({:EXIT, port, term}, ProcessState.t) :: no_return
  def handle_info({:EXIT, _, _}, _) do
    exit(:port_terminated)
  end
end
