# DatomicGenServer

An Elixir GenServer that communicates with a Clojure Datomic peer running in the 
JVM, using clojure-erlastic.

## Example

    data_to_add = [%{ 
        Db.id => Db.dbid(Db.schema_partition),
        Db.ident => :"person/name",
        Db.value_type => Db.type_string,
        Db.cardinality => Db.cardinality_one,
        Db.doc => "A person's name",
        Db.install_attribute => Db.schema_partition
    }]

    Db.transact(DatomicGenServer, data_to_add)

    # => {:ok, %DatomicGenServer.Db.DatomicTransaction{
            basis_t_after: 1001, 
            basis_t_before: 1000, 
            retracted_datoms: [],
            added_datoms: [%DatomicGenServer.Db.Datom{a: 50, added: true, e: 13194139534313, tx: 13194139534313,
               v: %Calendar.DateTime{abbr: "UTC", day: 14, hour: 5, min: 56, month: 2, sec: 53, std_off: 0, 
                                     timezone: "Etc/UTC", usec: 400000, utc_off: 0, year: 2016}},
              %DatomicGenServer.Db.Datom{a: 41, added: true, e: 66, tx: 13194139534313, v: 35},
              %DatomicGenServer.Db.Datom{a: 62, added: true, e: 66, tx: 13194139534313, v: "A person's name"},
              %DatomicGenServer.Db.Datom{a: 10, added: true, e: 66, tx: 13194139534313, v: :"person/name"},
              %DatomicGenServer.Db.Datom{a: 40, added: true, e: 66, tx: 13194139534313, v: 23},
              %DatomicGenServer.Db.Datom{a: 13, added: true, e: 0, tx: 13194139534313, v: 64}],      
           tempids: %{-9223367638809264705 => 66}}}

    query = [:find, Db.q?(:c), :where, [Db.q?(:c), :Db.doc, "A person's name"]]
    
    Db.q(DatomicGenServer, query)

    # => {:ok, #MapSet<['B']>}  # ASCII representation of ID 66

## Installation

If available in Hex, the package can be installed as follows:

  1. You will need to install the Clojure leiningen build tool in order to build
     the Clojure jar with which the application communicates. You will also need
     to have the datomic-pro peer jar installed in a local repository, with a 
     version matching the one specified in `priv/datomic_gen_server_peer/project.clj`.
     
     You can also specify your credentials for the my.datomic.com repo by
     creating a `~/.lein/credentials.clj.gpg` file. See the comments in 
     `priv/datomic_gen_server_peer/project.clj` for more details.
     
     The `mix compile` task includes running `lein uberjar` in the 
     `priv/datomic_gen_server_peer` directory, and `mix clean` will remove the 
     `target` subdirectory of that directory.

  2. Add datomic_gen_server to your list of dependencies in `mix.exs`:

        def deps do
          [{:datomic_gen_server, "~> 2.2.5"}]
        end
  
  3. You may want to create a config.exs file in your application that adds to 
     your application environment default values to control the default amount of 
     time the GenServer waits for the JVM to start before crashing, and the default 
     amount of time it waits for a reply from the JVM peer before crashing. See
     the `config/config.exs` file for an example.
     
  4. Ensure datomic_gen_server (as well as Logger and Calendar) is started before 
     your application:

        def application do
          [applications: [:logger, :calendar, :datomic_gen_server]]
        end
        
## Usage

See the Hex docs at [http://hexdocs.pm/datomic_gen_server/](http://hexdocs.pm/datomic_gen_server/).

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
to the Datomic API's `q`, `transact`, and `entity` functions. There are also
interface functions `migrate` and `seed` allowing you to migrate a database and
to seed it with test data.

A second API allows you to interact with Datomic using Elixir data structures as set 
out in the [Exdn project](http://github.com/psfblair/exdn) for translating between 
Elixir and edn; this API is exposed by `DatomicGenServer.Db`. The results of query
functions such as `q` and `pull` are translated back to Elixir data structures 
using Exdn's "irreversible" data translators, which can also accept converter 
functions that will transform the data into your own structures or custom formats
(see the tests for examples). The results of `transact` are returned in a
`DatomicTransaction` struct; the datoms are returned in `Datom` structs. The
results of `seed` are also encoded in a `DatomicTransaction` struct.

The `entity` functions in both `DatomicGenServer` and `DatomicGenServer.Db` allow 
passing in a list of keys representing the attributes you wish to fetch, or `:all` 
if you want all of them.

The `DatomicGenServer.Db` module also contains shortcuts for many common Datomic
keys and values, which would otherwise require a lot of additional punctuation 
in Elixir.

## Entity Maps

The `DatomicGenServer.EntityMap` module provides a data structure for holding 
entity data as a map of maps or structs. The keys of the map are Datomic 
entity IDs by default, but you can choose any attribute as the index. Entity
data is by default stored as a map of Datomic attributes to values; however,
you can choose to have the data placed into a struct of your choosing. You can
also supply a translation map in case the field names of the struct do not
match the names of the Datomic attributes they correspond with. (Make sure to
read the Hex docs for important caveats about using this data structure.)

## Mocking Connections

The `DatomicGenServer` module offers functions that allow you to create and use 
mock connections based on database snapshots. Mocking connections requires that  
the `:allow_datomic_mocking?` configuration parameter be set in the 
`:datomic_gen_server` application environment. 

The `mock` function saves a snapshot of the current database value using a key 
that you provide, and creates a new mock connection using that database as a 
starting point. Subsequent operations on the database will use that mock 
connection until you call either the `reset` or `unmock` functions. The `reset`
function accepts a key and retrieves the database snapshot saved under that
key; a new mock connection is created using that snapshot as the starting point.
The `unmock` function reverts back to the real, live connection and database.

## Limitations

This code has not been checked for possible injection vulnerabilities. Use at 
your own risk.

Currently all interaction with Datomic is synchronous and there is no support for
Datomic functions such as `transact-async`; furthermore, while the GenServer's 
`handle_cast` callback will send requests to the Clojure peer, the responses from
the peer will be discarded. Implementing support for `transact-async` and `handle_cast` 
may be somewhat complicated owing to the way in which the GenServer waits for replies 
from the Clojure peer (see the comments on `DatomicGenServer.wait_for_reply`). 

Queries and transactions are passed to the Clojure peer as edn strings, and results 
come back as edn strings. Certain Datomic APIs return references to Java objects,
which can't be manipulated from Elixir (e.g., a call to `entity` returns a 
dynamic map); where possible the related data is translated to edn to be 
returned to the GenServer.

There may be ways to take better advantage of clojure-erlastic to serialize data 
structures directly; however, clojure-erlastic's serialization format is different 
from that of Exdn, which is more suited to Datomic operations. It isn't clear 
whether an additional translation layer would be worthwhile.

## License

```
The MIT License (MIT)

Copyright (c) 2016 Paul Blair

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
