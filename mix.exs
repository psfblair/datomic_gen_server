defmodule DatomicGenServer.Mixfile do
  use Mix.Project

  def project do
    [app: :datomic_gen_server,
     version: "1.0.0",
     elixir: "~> 1.2",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps,
     aliases: aliases]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger, :calendar]] # Calendar needed for edn conversions of #inst
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [ {:exdn,    "~> 2.1.2"},
      {:dialyxir, "~> 0.3", only: [:dev]}]
  end

  defp aliases do
    [ {:clean,   ["clean",   &clean_uberjars/1]},
      {:compile, ["compile", &uberjar/1]}
    ]
  end

  defp clean_uberjars(_) do
    priv_dir = Path.join [System.cwd(), "priv"]

    uberjar_dir = Path.join [priv_dir, "datomic_gen_server_peer", "target" ]
    if File.exists?(uberjar_dir) do
      File.rm_rf uberjar_dir
    end
  end

  defp uberjar(_) do
    peer_dir = Path.join [:code.priv_dir(:datomic_gen_server), "datomic_gen_server_peer" ]
    if [peer_dir, "target", "peer*standalone.jar"] |> Path.join |> Path.wildcard |> Enum.empty? do
      pwd = System.cwd()
      File.cd(peer_dir)
      Mix.shell.cmd "lein uberjar"
      File.cd(pwd)
    end
  end
end
