Mix.Task.run("app.start", [])

old = File.read!("priv/plans/domains/blocks_world.jsonld") |> Jason.decode!()
{:ok, new_json_str} = Taskweft.DSL.compile(File.read!("priv/plans/domains/blocks_world_dsl.ex"))
new = Jason.decode!(new_json_str)

# Compare old vs new key by key, printing ONLY differences
for k <- Map.keys(old) ++ Map.keys(new) |> Enum.uniq() |> Enum.sort() do
  ov = Map.get(old, k)
  nv = Map.get(new, k)
  if ov != nv do
    IO.puts("❌ #{k}:")
    IO.puts("  old: #{Jason.encode!(ov)}")
    IO.puts("  new: #{Jason.encode!(nv)}")
  else
    IO.puts("✅ #{k}")
  end
end