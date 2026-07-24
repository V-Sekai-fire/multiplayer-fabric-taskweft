Mix.Task.run("app.start", [])

old = File.read!("priv/plans/domains/blocks_world.jsonld") |> Jason.decode!()
{:ok, new_json} = Taskweft.DSL.compile(File.read!("priv/plans/domains/blocks_world_dsl.ex"))
new = Jason.decode!(new_json)

IO.puts("=== Actions body diff ===")
for {name, oa} <- old["actions"] do
  na = new["actions"][name]
  {:ok, oe} = Jason.encode(oa["body"])
  {:ok, ne} = Jason.encode(na["body"])
  if oe != ne do
    IO.puts("❌ #{name} body differs")
    for {ob, i} <- Enum.with_index(oa["body"]) do
      nb = Enum.at(na["body"], i)
      {:ok, oe2} = Jason.encode(ob)
      {:ok, ne2} = Jason.encode(nb)
      if oe2 != ne2 do
        IO.puts("  [#{i}] old: #{oe2}")
        IO.puts("      new: #{ne2}")
      end
    end
  else
    IO.puts("✅ #{name} body")
  end
end

IO.puts("\n=== Variables diff ===")
{:ok, ov} = Jason.encode(old["variables"])
{:ok, nv} = Jason.encode(new["variables"])
if ov != nv do
  IO.puts("❌ variables differ")
  for {ov_item, i} <- Enum.with_index(old["variables"]) do
    nv_item = Enum.at(new["variables"], i)
    {:ok, oe} = Jason.encode(ov_item)
    {:ok, ne} = Jason.encode(nv_item)
    if oe != ne do
      IO.puts("  [#{i}] old: #{oe}")
      IO.puts("      new: #{ne}")
    end
  end
else
  IO.puts("✅ variables")
end

IO.puts("\n=== Methods diff ===")
for {name, om} <- old["methods"] do
  nm = new["methods"][name]
  {:ok, oe} = Jason.encode(om)
  {:ok, ne} = Jason.encode(nm)
  if oe != ne do
    IO.puts("❌ #{name}:")
    IO.puts("  old: #{oe}")
    IO.puts("  new: #{ne}")
  else
    IO.puts("✅ #{name}")
  end
end