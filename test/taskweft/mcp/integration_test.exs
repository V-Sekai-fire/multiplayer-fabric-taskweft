# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Taskweft.MCP.IntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Taskweft.MCP.Client

  setup_all do
    {:ok, _} = Application.ensure_all_started(:ex_mcp)
    Application.ensure_all_started(:inets)
    Application.ensure_all_started(:ssl)

    port = (20000 + :erlang.unique_integer([:positive])) |> rem(40000)
    server_opts = [transport: :http, port: port, host: "127.0.0.1"]
    {:ok, server} = Taskweft.MCP.Server.start_link(server_opts)
    url = "http://127.0.0.1:#{port}"

    client = connect_with_retry(url)

    on_exit(fn ->
      Client.disconnect(client)
      Process.unlink(server)
      Process.exit(server, :shutdown)
    end)

    {:ok, %{client: client}}
  end

  defp connect_with_retry(url, retries \\ 20) do
    case Client.connect(
           {:http, url: url, endpoint: "/mcp/v1", use_sse: false},
           timeout: 10_000
         ) do
      {:ok, client} ->
        client

      {:error, _reason} when retries > 0 ->
        Process.sleep(100)
        connect_with_retry(url, retries - 1)

      {:error, reason} ->
        raise "Could not connect after retries: #{inspect(reason)}"
    end
  end

  describe "plan tool" do
    test "returns golden plan for blocks_world", %{client: client} do
      domain_dsl = File.read!("priv/plans/domains/blocks_world_dsl.ex")

      golden =
        Jason.decode!(
          File.read!("priv/plans/expected/blocks_world__blocks_world_1a_expected.json")
        )

      {:ok, content} = Client.call_tool(client, "plan", %{domain_dsl: domain_dsl})
      plan = decode_plan(content)

      assert plan["plan"] == golden["plan"],
             "blocks_world: plan mismatch"
    end

    test "returns golden plan for entity_capabilities", %{client: client} do
      domain = File.read!("priv/plans/domains/entity_capabilities.jsonld")
      problem = File.read!("priv/plans/problems/entity_caps_drone.jsonld")

      golden =
        Jason.decode!(
          File.read!("priv/plans/expected/entity_capabilities__entity_caps_drone_expected.json")
        )

      merged =
        Jason.decode!(domain) |> Map.merge(Jason.decode!(problem)) |> Jason.encode!()

      {:ok, content} = Client.call_tool(client, "plan", %{domain_json: merged})
      plan = decode_plan(content)
      assert plan["plan"] == golden["plan"]
    end

    test "returns golden plan for healthcare", %{client: client} do
      domain = File.read!("priv/plans/domains/healthcare.jsonld")
      problem = File.read!("priv/plans/problems/healthcare_one.jsonld")

      golden =
        Jason.decode!(File.read!("priv/plans/expected/healthcare__healthcare_one_expected.json"))

      merged =
        Jason.decode!(domain) |> Map.merge(Jason.decode!(problem)) |> Jason.encode!()

      {:ok, content} = Client.call_tool(client, "plan", %{domain_json: merged})
      plan = decode_plan(content)
      assert plan["plan"] == golden["plan"]
    end

    test "returns error for invalid JSON", %{client: client} do
      assert {:error, _reason} =
               Client.call_tool(client, "plan", %{domain_json: "not valid json"})
    end
  end

  describe "list_tools" do
    test "exposes plan and validate tools", %{client: client} do
      {:ok, tools} = Client.list_tools(client)
      names = Enum.map(tools, &(&1[:name] || Map.get(&1, "name")))

      assert "plan" in names
      assert "validate" in names
    end
  end

  describe "validate tool" do
    test "validates a correct domain", %{client: client} do
      domain = File.read!("priv/plans/domains/blocks_world.jsonld")
      assert {:ok, _content} = Client.call_tool(client, "validate", %{domain_json: domain})
    end

    test "rejects a malformed domain", %{client: client} do
      assert {:error, _reason} =
               Client.call_tool(client, "validate", %{domain_json: "{}"})
    end
  end

  # ── helpers ──

  defp decode_plan(content) do
    content
    |> List.first()
    |> Map.get(:text)
    |> Jason.decode!()
  end
end
