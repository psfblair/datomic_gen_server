# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

# Default time to wait for the JVM to startup; crash if it is exceeded.
# This can be overridden by passing a different value to start_link.
config :datomic_gen_server, startup_wait_millis: 15_000

# Default time to wait for a message to come back from the JVM before crashing.
# This value can be overridden on a per-message basis by passing a timeout
# to the client functions in the module.
config :datomic_gen_server, message_wait_until_crash: 5_000

# Default time for the client functions in the API to wait for a message to 
# come back from the JVM before failing. Note that if the client call fails
# before the GenServer crashes (based on the message_wait_until_crash setting)
# the GenServer will not crash due to the non-receipt of that message.
# This value can be overridden on a per-message basis by passing a timeout_on_call
# parameter to the client functions in the module. However, if this is set 
# to be longer than the message_wait_until_crash value, it will have no effect,
# since the GenServer will crash first.
config :datomic_gen_server, timeout_on_call: 20_000

# Generally you'll want to allow Datomic mocking in test environments only.
# config :datomic_gen_server, :allow_datomic_mocking? true
