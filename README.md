# DatomicGenServer

An Elixir GenServer that communicates with a Clojure Datomic peer running in the 
JVM, using clojure-erlastic.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. You will need to install the Clojure leiningen build tool in order to build
     the Clojure jar with which the application communicates. You will also need
     to have the datomic-pro peer jar installed in a local repository, with a 
     version matching the one specified in priv/datomic_gen_server_peer/project.clj.
     
     The `mix compile` task includes running `lein uberjar` in the 
     `priv/datomic_gen_server_peer` directory, and `mix clean` will remove the `target` subdirectory of that directory.

  2. Add datomic_gen_server to your list of dependencies in `mix.exs`:

        def deps do
          [{:datomic_gen_server, "~> 1.0.0"}]
        end
  
  3. You may want to create a config.exs file in your application that adds to 
     your application environment default values to control the default amount of 
     time the GenServer waits for the JVM to start before crashing, and the default 
     amount of time it waits for a reply from the JVM peer before crashing. See
     the config/config.exs file for an example.
     
  4. Ensure datomic_gen_server (as well as Logger and Calendar) is started before 
     your application:

        def application do
          [applications: [:logger, :calendar, :datomic_gen_server]]
        end
        
## Usage

Start the server by calling `DatomicGenServer.start` or `DatomicGenServer.start_link`.
These functions accept the URL of the Datomic transactor to which to connect, a
boolean parameter indicating whether or not to create the database if it does not
yet exist, and a keyword list of options. The options may include the normal
options accepted by `GenServer.start` and `GenServer.start_link`, as well as 
options to control the default wait times after which the server will crash.

On start, the server will send itself an initial message to start the JVM, then 
register itself under any alias provided in the options. Any subsequent message
sent to the server will arrive after the initialization message, and will need
to wait until initialization is complete. Thus, it is important that the timeouts
on messages sent to the server exceed the startup timeout value, at least for the
messages sent during the startup phase.

Two interfaces for interacting with Datomic are exposed. With one, you communicate
back and forth with Datomic using edn strings; this API is exposed by 
`DatomicGenServer`. Currently there are three interface functions corresponding
to the Datomic API's `q`, `transact`, and `entity` functions.

A second API allows you to interact with Datomic using Elixir data structures as set 
out in the [Exdn project](http://github.com/psfblair/exdn) for translating between 
Elixir and edn; this API is exposed by `DatomicGenServer.Db`. The results of query
functions such as `q` and `entity` are translated back to Elixir data structures 
using Exdn's "irreversible" data translators, which can also accept converter 
functions that will transform the data into your own structures or custom formats
(see the tests for examples). The results of `transact` are returned in a
`DatomicTransaction` struct; the datoms are returned in `Datom` structs.

The `entity` functions in both `DatomicGenServer` and `DatomicGenServer.Db` allow 
passing in a list of keys representing the attributes you wish to fetch, or `:all` 
if you want all of them.

## Limitations

Currently all interaction with Datomic is synchronous and there is no support for
functions such as `transact-async`. Implementing this support may be somewhat
complicated owing to the way in which the GenServer waits for replies from the
Clojure peer (see the comments on `DatomicGenServer.wait_for_reply`).

Queries and transactions are passed to Datomic as edn strings, and results come
back as edn strings. Certain of the Datomic APIs return references to Java 
objects, which can't be manipulated from Elixir (e.g., a call to `entity` returns
a dynamic map); where possible the related data is translated to edn to be 
returned to the GenServer.

There may be ways to take better advantage of clojure-erlastic to serialize data 
structures directly; however, clojure-erlastic's serialization format is different 
from that of Exdn, which is more suited to Datomic operations. It isn't clear 
whether an additional translation layer would be worthwhile.
