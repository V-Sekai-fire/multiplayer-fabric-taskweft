Mix.Task.run("app.start", [])

old = File.read!("priv/plans/domains/blocks_world.jsonld")
old_map = Jason.decode!(old)

{:ok, new_json} = Taskweft.DSL.compile(File.read!("priv/plans/domains/blocks_world_dsl.ex"))
new_map = Jason.decode!(new_json)

# Compare top-level keys
for k <- Map.keys(old_map) |> Enum.concat(Map.keys(new_map)) |> Enum.uniq() do
  old_v = Map.get(old_map, k)
  new_v = Map.get(new_map, k)
  if old_v != new_v do
    IO.puts("DIFF #{k}:")
    IO.puts("  old: #{inspect(old_v, limit: 2)}")
    IO.puts("  new: #{inspect(new_v, limit: 2)}")
    IO.puts("")
  end
end