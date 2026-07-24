Mix.Task.run("app.start", [])

alias Taskweft.MCP.Client

IO.puts("Connecting to local MCP server...")
{:ok, client} = Client.connect("http://127.0.0.1:51737")
IO.puts("Connected.")

{:ok, tools} = Client.list_tools(client)
IO.puts("Tools: #{Enum.map_join(tools, ", ", & &1.name)}")

dsl = File.read!("priv/plans/domains/blocks_world_dsl.ex")
IO.puts("\nCalling plan tool with DSL domain...")

case Client.call_tool(client, "plan", %{
  "domain_dsl" => dsl,
  "format" => "dsl",
  "explain" => false
}) do
  {:ok, content} ->
    text = case content do
      [%{"text" => t}] -> t
      [%{text: t}] -> t
      [t] when is_binary(t) -> t
      _ -> inspect(content)
    end
    plan = Jason.decode!(text)
    steps = Map.get(plan, "plan", [])
    IO.puts("OK: #{length(steps)} plan steps")
    Enum.each(steps, fn step -> IO.puts("  #{inspect(step)}") end)

  {:error, reason} ->
    IO.puts("FAIL: #{inspect(reason)}")
end

Client.disconnect(client)
IO.puts("Done.")
