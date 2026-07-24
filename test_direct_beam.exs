Mix.Task.run("app.start", [])

IO.puts("=== TEST 1: Direct BEAM call (no MCP) ===")
IO.puts("")

domains = Path.wildcard("priv/plans/domains/*_dsl.ex")

Enum.each(domains, fn path ->
  name = Path.basename(path)
  dsl_source = File.read!(path)

  case Taskweft.DSL.compile(dsl_source) do
    {:ok, json} ->
      case Taskweft.plan(json) do
        {:ok, plan} ->
          plan_map = Jason.decode!(plan)
          steps = Map.get(plan_map, "plan", [])
          status = if is_list(steps) and length(steps) > 0, do: "✓", else: "⚠"
          IO.puts("  #{status} #{name}: #{length(steps)} plan steps")

        {:error, reason} ->
          IO.puts("  ✗ #{name}: plan error: #{reason}")
      end

    {:error, reason} ->
      IO.puts("  ✗ #{name}: DSL compile error: #{reason}")
  end
end)

IO.puts("")
IO.puts("=== TEST 2: MCP tool call via BEAM client ===")

alias Taskweft.MCP.Client

{:ok, client} = Client.connect("http://127.0.0.1:51737")
IO.puts("✓ MCP client connected")

{:ok, tools} = Client.list_tools(client)
IO.puts("✓ tools/list: #{length(tools)} tools")

# Test plan tool
dsl_source = File.read!("priv/plans/domains/blocks_world_dsl.ex")

case Client.call_tool(client, "plan", %{
  "domain_dsl" => dsl_source,
  "format" => "dsl"
}) do
  {:ok, content} ->
    text = case content do
      [%{"text" => t}] -> t
      [%{text: t}] -> t
      _ -> inspect(content)
    end
    IO.puts("✓ MCP plan call returned:")
    IO.puts("  #{String.slice(text, 0, 300)}")

  {:error, reason} ->
    IO.puts("✗ MCP plan call: #{inspect(reason)}")
end

Client.disconnect(client)
IO.puts("✓ Done")