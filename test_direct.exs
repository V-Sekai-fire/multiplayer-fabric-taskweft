Mix.Task.run("app.start", [])

dsl = File.read!("priv/plans/domains/blocks_world_dsl.ex")
{:ok, json} = Taskweft.DSL.compile(dsl)
IO.puts("=== Compiled JSON ===")
decoded = Jason.decode!(json)
IO.puts("actions keys: #{inspect(Map.keys(Map.get(decoded, "actions", %{})))}")
IO.puts("methods keys: #{inspect(Map.keys(Map.get(decoded, "methods", %{})))}")

IO.puts("\n=== Full JSON (first 500) ===")
IO.puts(String.slice(json, 0, 500))

case Taskweft.plan(json) do
  {:ok, plan_json} ->
    plan = Jason.decode!(plan_json)
    steps = Map.get(plan, "plan", [])
    IO.puts("\n=== Plan: #{length(steps)} steps ===")
    Enum.each(steps, fn step -> IO.puts("  #{inspect(step)}") end)
  {:error, reason} ->
    IO.puts("\n=== Plan error: #{inspect(reason)} ===")
end