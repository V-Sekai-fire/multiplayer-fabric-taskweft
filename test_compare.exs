Mix.Task.run("app.start", [])

old = File.read!("priv/plans/domains/blocks_world.jsonld") |> Jason.decode!()
{:ok, new_json} = Taskweft.DSL.compile(File.read!("priv/plans/domains/blocks_world_dsl.ex"))
new = Jason.decode!(new_json)

IO.puts("=== Top-level key diffs ===")
for k <- Map.keys(old) ++ Map.keys(new) |> Enum.uniq() |> Enum.sort() do
  ov = Map.get(old, k)
  nv = Map.get(new, k)
  if ov != nv do
    IO.puts("❌ #{k}: len(old)=#{length(Map.keys(ov || %{}))} len(new)=#{length(Map.keys(nv || %{}))}")
  else
    IO.puts("✅ #{k}")
  end
end

IO.puts("\n=== Action body diffs ===")
for action <- Map.keys(old["actions"]) do
  old_body = old["actions"][action]["body"]
  new_body = new["actions"][action]["body"]
  if old_body != new_body do
    IO.puts("❌ #{action} body differs")
    for {i, ob} <- Enum.with_index(old_body) do
      nb = Enum.at(new_body, i)
      if ob != nb do
        IO.puts("  [#{i}]")
        IO.puts("    old: #{Jason.encode!(ob)}")
        IO.puts("    new: #{Jason.encode!(nb)}")
      end
    end
  else
    IO.puts("✅ #{action}")
  end
end

IO.puts("\n=== Variables diffs ===")
old_vars = old["variables"]
new_vars = new["variables"]
if old_vars != new_vars do
  IO.puts("❌ count: old=#{length(old_vars)} new=#{length(new_vars)}")
  for {ov, i} <- Enum.with_index(old_vars) do
    nv = Enum.at(new_vars, i)
    if ov != nv do
      IO.puts("  [#{i}]")
      IO.puts("    old: #{Jason.encode!(ov)}")
      IO.puts("    new: #{Jason.encode!(nv)}")
    end
  end
else
  IO.puts("✅")
end