Mix.Task.run("app.start", [])

old = File.read!("priv/plans/domains/blocks_world.jsonld") |> Jason.decode!()
{:ok, new_json} = Taskweft.DSL.compile(File.read!("priv/plans/domains/blocks_world_dsl.ex"))
new = Jason.decode!(new_json)

IO.puts("=== todo_list === equal=#{inspect(old["todo_list"] == new["todo_list"])}")
IO.puts("=== name === equal=#{inspect(old["name"] == new["name"])}")
IO.puts("=== @type === equal=#{inspect(old["@type"] == new["@type"])}")
IO.puts("=== @context === equal=#{inspect(old["@context"] == new["@context"])}")
IO.puts("=== actions keys === equal=#{inspect(Map.keys(old["actions"]) == Map.keys(new["actions"]))}")
IO.puts("=== methods keys === equal=#{inspect(Map.keys(old["methods"]) == Map.keys(new["methods"]))}")
IO.puts("=== variables === equal=#{inspect(old["variables"] == new["variables"])}")

# Check first action body element format
old_body0 = hd(old["actions"]["a_pickup"]["body"])
new_body0 = hd(new["actions"]["a_pickup"]["body"])
IO.puts("\na_pickup body[0]:")
IO.puts("  old has eval key: #{inspect(Map.has_key?(old_body0, "eval"))}")
IO.puts("  new has eval key: #{inspect(Map.has_key?(new_body0, "eval"))}")
IO.puts("  old: #{Jason.encode!(old_body0)}")
IO.puts("  new: #{Jason.encode!(new_body0)}")