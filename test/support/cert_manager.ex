defmodule Taskweft.Test.CertManager do
  @moduledoc """
  Reads and writes CRDB TLS credentials to the macOS Keychain.

  Certs are stored as generic passwords under service "multiplayer-fabric-crdb":
    account "ca-cert"      → CA certificate PEM
    account "client-cert"  → client certificate PEM
    account "client-key"   → client private key PEM

  Setup (run once):
    Taskweft.Test.CertManager.import_from_files(ca_path, cert_path, key_path)
  Or use scripts/setup_keychain_certs.sh from the repo root.
  """

  @service "multiplayer-fabric-crdb"

  @spec ssl_opts(charlist()) :: keyword() | nil
  def ssl_opts(sni \\ ~c"localhost") do
    with {:ok, ca_pem}   <- fetch("ca-cert"),
         {:ok, cert_pem} <- fetch("client-cert"),
         {:ok, key_pem}  <- fetch("client-key") do
      [
        cacerts:                 decode_certs(ca_pem),
        cert:                    decode_cert(cert_pem),
        key:                     decode_key(key_pem),
        server_name_indication:  sni,
        verify:                  :verify_peer
      ]
    else
      _ -> nil
    end
  end

  @spec import_from_files(Path.t(), Path.t(), Path.t()) :: :ok | {:error, term()}
  def import_from_files(ca_path, cert_path, key_path) do
    with {:ok, ca}   <- File.read(ca_path),
         {:ok, cert} <- File.read(cert_path),
         {:ok, key}  <- File.read(key_path),
         :ok         <- store("ca-cert", ca),
         :ok         <- store("client-cert", cert),
         :ok         <- store("client-key", key) do
      :ok
    end
  end

  defp fetch(account) do
    case System.cmd("security",
           ["find-generic-password", "-a", account, "-s", @service, "-w"],
           stderr_to_stdout: false) do
      {b64, 0} ->
        case Base.decode64(String.trim(b64)) do
          {:ok, pem} -> {:ok, pem}
          :error     -> :not_found
        end

      _ ->
        :not_found
    end
  end

  defp store(account, pem) do
    System.cmd("security", ["delete-generic-password", "-a", account, "-s", @service],
               stderr_to_stdout: true)
    b64 = Base.encode64(pem)
    case System.cmd("security",
           ["add-generic-password", "-a", account, "-s", @service, "-w", b64]) do
      {_, 0}       -> :ok
      {msg, code}  -> {:error, {code, String.trim(msg)}}
    end
  end

  defp decode_certs(pem) do
    pem
    |> :public_key.pem_decode()
    |> Enum.filter(fn {type, _, _} -> type == :Certificate end)
    |> Enum.map(fn {_, der, _} -> der end)
  end

  defp decode_cert(pem) do
    [{_, der, _}] =
      pem
      |> :public_key.pem_decode()
      |> Enum.filter(fn {type, _, _} -> type == :Certificate end)
    der
  end

  defp decode_key(pem) do
    [{type, der, _} | _] = :public_key.pem_decode(pem)
    {type, der}
  end
end
