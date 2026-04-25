defmodule Taskweft.Test.DBHelpers do
  @moduledoc """
  Shared helper for starting a Postgrex pool in property tests.

  Reads connection options from environment variables so that the same
  test code works against both insecure (dev) and TLS-secured (CI/Docker)
  CockroachDB instances.

  ## Environment variables

  | Variable            | Purpose                                             |
  |---------------------|-----------------------------------------------------|
  | `TEST_DATABASE_URL` | Full Postgres URL (used when set; overrides all)    |
  | `TEST_DB_HOST`      | Hostname (default: localhost)                       |
  | `TEST_DB_PORT`      | Port    (default: 26257)                            |
  | `TEST_DB_NAME`      | Database name (default: taskweft_test)              |
  | `TEST_DB_USER`      | Username (default: root)                            |
  | `TEST_DB_CA_CERT`   | Path to CA cert (enables TLS when set)              |
  | `TEST_DB_CERT`      | Path to client cert (required with CA cert)         |
  | `TEST_DB_KEY`       | Path to client key  (required with CA cert)         |
  | `TEST_DB_SNI`       | TLS server name indication (default: localhost)     |

  If none of the above are set and the default cert location exists
  (`multiplayer-fabric-hosting/certs/crdb/`), TLS options are inferred
  automatically so local development with the Docker stack works without
  any extra environment setup.
  """

  @default_crdb_certs_dir Path.expand(
    "../../../../multiplayer-fabric-hosting/certs/crdb",
    __DIR__
  )

  @spec start_pool(atom()) :: {:ok, pid()} | {:error, term()}
  def start_pool(name) do
    Postgrex.start_link([name: name] ++ connection_opts())
  end

  @spec connection_opts() :: keyword()
  def connection_opts do
    case System.get_env("TEST_DATABASE_URL") do
      url when is_binary(url) and url != "" ->
        [url: url]

      _ ->
        tls_opts()
    end
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp tls_opts do
    ca   = env_or("TEST_DB_CA_CERT",   Path.join(@default_crdb_certs_dir, "ca.crt"))
    cert = env_or("TEST_DB_CERT",      Path.join(@default_crdb_certs_dir, "client.root.crt"))
    key  = env_or("TEST_DB_KEY",       Path.join(@default_crdb_certs_dir, "client.root.key"))
    host = System.get_env("TEST_DB_HOST", "localhost")
    port = System.get_env("TEST_DB_PORT", "26257") |> String.to_integer()
    db   = System.get_env("TEST_DB_NAME", "taskweft_test")
    user = System.get_env("TEST_DB_USER", "root")
    sni  = System.get_env("TEST_DB_SNI",  "localhost") |> String.to_charlist()

    if File.exists?(ca) do
      [
        hostname: host,
        port: port,
        database: db,
        username: user,
        ssl: [
          cacertfile: ca,
          certfile: cert,
          keyfile: key,
          server_name_indication: sni,
          verify: :verify_peer
        ]
      ]
    else
      [
        hostname: host,
        port: port,
        database: db,
        username: user
      ]
    end
  end

  defp env_or(var, default), do: System.get_env(var, default)
end
