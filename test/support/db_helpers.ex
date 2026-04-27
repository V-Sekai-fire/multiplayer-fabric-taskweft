defmodule Taskweft.Test.DBHelpers do
  @moduledoc """
  Shared helper for starting a Postgrex pool in property tests.

  Connection resolution order:
    1. `TEST_DATABASE_URL` env var — full Postgres URL, used as-is.
    2. macOS Keychain via `Taskweft.Test.CertManager` — TLS creds stored under
       service "multiplayer-fabric-crdb" (see scripts/setup_keychain_certs.sh).
    3. Plain insecure connection — falls back when no creds are available.

  Host/port/db/user can be overridden with:
    TEST_DB_HOST  (default: localhost)
    TEST_DB_PORT  (default: 26257)
    TEST_DB_NAME  (default: taskweft_test)
    TEST_DB_USER  (default: root)
    TEST_DB_SNI   (default: localhost)
  """

  alias Taskweft.Test.CertManager

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
        base_opts() ++ ssl_opts()
    end
  end

  defp base_opts do
    [
      hostname: System.get_env("TEST_DB_HOST", "localhost"),
      port:     System.get_env("TEST_DB_PORT", "26257") |> String.to_integer(),
      database: System.get_env("TEST_DB_NAME", "taskweft_test"),
      username: System.get_env("TEST_DB_USER", "root")
    ]
  end

  defp ssl_opts do
    sni = System.get_env("TEST_DB_SNI", "localhost") |> String.to_charlist()
    case CertManager.ssl_opts(sni) do
      nil  -> []
      opts -> [ssl: opts]
    end
  end
end
