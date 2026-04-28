defmodule Taskweft.HRR.TableServer do
  use GenServer

  def start_link(opts) do
    {name, rest} = Keyword.pop!(opts, :name)
    GenServer.start_link(__MODULE__, {name, rest}, name: name)
  end

  @impl GenServer
  def init({name, opts}) do
    path = Keyword.get(opts, :path, Path.join(System.tmp_dir!(), "hrr_#{name}.dets"))
    File.mkdir_p!(Path.dirname(path))
    {:ok, _} = :dets.open_file(name, [{:file, String.to_charlist(path)}, {:type, :set}])
    {:ok, name}
  end

  @impl GenServer
  def terminate(_reason, name) do
    :dets.close(name)
  end

  @impl GenServer
  def handle_call(:ping, _from, state), do: {:reply, :ok, state}
end
