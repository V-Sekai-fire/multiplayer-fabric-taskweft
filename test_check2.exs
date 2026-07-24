Mix.Task.run("app.start", [])

old = File.read!("priv/plans/domains/blocks_world.jsonld") |> Jason.decode!()
{:ok, new_json} = Taskweft.DSL.compile(File.read!("priv/plans/domains/blocks_world_dsl.ex"))
new = Jason.decode!(new_json)

IO.puts("=== Test plan with real DSL output ===")
case Taskweft.plan(new_json) do
  {:ok, plan} -> IO.puts("✅ #{plan |> Jason.decode!() |> Map.get("plan", []) |> length()} steps")
  {:error, r} -> IO.puts("❌ #{inspect(r)}")
end

IO.puts("\n=== Test plan with old JSON-LD ===")
case Taskweft.plan(File.read!("priv/plans/domains/blocks_world.jsonld")) do
  {:ok, plan} -> IO.puts("✅ #{plan |> Jason.decode!() |> Map.get("plan", []) |> length()} steps")
  {:error, r} -> IO.puts("❌ #{inspect(r)}")
end

# Check ALL action body items for format differences
IO.puts("\n=== Action body format check ===")
all_ok = true
for {name, oa} <- old["actions"] do
  na = new["actions"][name]
  for {ob, i} <- Enum.with_index(oa["body"]) do
    nb = Enum.at(na["body"], i)
    if ob != nb do
      if all_ok do
        IO.puts("First diff at #{name} body[#{i}]:")
        IO.puts("  old: #{Jason.encode!(ob)}")
        IO.puts("  new: #{Jason.encode!(nb)}")
      end
      all_ok = false
    end
  end
end
if all_ok, do: IO.puts("✅ All action bodies match")

# Check method alternatives
IO.puts("\n=== Method alternatives check ===")
all_ok = true
for {name, om} <- old["methods"] do
  nm = new["methods"][name]
  for {oa, i} <- Enum.with_index(om["alternatives"]) do
    na = Enum.at(nm["alternatives"], i)
    if oa != na do
      if all_ok do
        IO.puts("First diff at #{name} alt[#{i}]:")
        IO.puts("  old: #{Jason.encode!(oa)}")
        IO.puts("  new: #{Jason.encode!(na)}")
      end
      all_ok = false
    end
  end
end
if all_ok, do: IO.puts("✅ All method alternatives match")