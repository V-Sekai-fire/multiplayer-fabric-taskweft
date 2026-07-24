Mix.Task.run("app.start", [])

old = File.read!("priv/plans/domains/blocks_world.jsonld") |> Jason.decode!()
{:ok, new_json} = Taskweft.DSL.compile(File.read!("priv/plans/domains/blocks_world_dsl.ex"))
new = Jason.decode!(new_json)

IO.puts("=== Check all keys deep equal ===:")

# Re-wrap eval body items in our new to match old format before comparing
new_fixed = put_in(new, ["actions", "a_pickup", "body"], 
  Enum.map(new["actions"]["a_pickup"]["body"], fn item ->
    if Map.has_key?(item, "eval"), do: item, else: %{"eval" => item}
  end))

IO.puts("a_pickup body[0] old: #{Jason.encode!(hd(old["actions"]["a_pickup"]["body"]))}")
IO.puts("a_pickup body[0] new: #{Jason.encode!(hd(new_fixed["actions"]["a_pickup"]["body"]))}")

# Now check plan with fixed version
fixed_json = Jason.encode!(new_fixed)
case Taskweft.plan(fixed_json) do
  {:ok, plan} -> IO.puts("✅ PLAN FOUND: #{plan |> Jason.decode!() |> Map.get("plan", []) |> length()} steps")
  {:error, reason} -> IO.puts("❌ #{inspect(reason)}")
end

# Check variables diff
IO.puts("\n=== Variables ===")
for {ov, i} <- Enum.with_index(old["variables"]) do
  nv = Enum.at(new["variables"], i)
  if ov != nv do
    IO.puts("[#{i}] differs:")
    for k <- Map.keys(ov) ++ Map.keys(nv) |> Enum.uniq() do
      if Map.get(ov, k) != Map.get(nv, k) do
        IO.puts("  #{k}: old=#{inspect(Map.get(ov, k))} new=#{inspect(Map.get(nv, k))}")
      end
    end
  else
    IO.puts("[#{i}] ✅")
  end
end