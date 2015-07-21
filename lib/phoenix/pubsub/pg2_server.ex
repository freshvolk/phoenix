defmodule Phoenix.PubSub.PG2Server do
  @moduledoc false

  use GenServer
  alias Phoenix.PubSub.Local

  def start_link(opts) do
    GenServer.start_link __MODULE__, opts, name: Dict.fetch!(opts, :name)
  end

  def init(opts) do
    server_name   = Keyword.fetch!(opts, :name)
    local_name    = Keyword.fetch!(opts, :local_name)
    local_pid     = Process.whereis(local_name)
    pg2_namespace = pg2_namespace(server_name)

    :ok = :pg2.create(pg2_namespace)
    :ok = :pg2.join(pg2_namespace, local_pid)

    {:ok, %{local_pid: local_pid, namespace: pg2_namespace}}
  end

  def handle_call({:subscribe, pid, topic, opts}, _from, state) do
    response = {:perform, {Local, :subscribe, [state.local_pid, pid, topic, opts]}}
    {:reply, response, state}
  end

  def handle_call({:unsubscribe, pid, topic}, _from, state) do
    response = {:perform, {Local, :unsubscribe, [state.local_pid, pid, topic]}}
    {:reply, response, state}
  end

  def handle_call({:broadcast, from_pid, topic}, _from, state) do
    case :pg2.get_members(state.namespace) do
      {:error, {:no_such_group, _}} ->
        {:stop, :no_such_group, {:error, :no_such_group}, state}

      pids when is_list(pids) ->
        {:reply, {:perform, {__MODULE__, :forward_to_local, [pids, from_pid, topic]}}, state}
    end
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def forward_to_local(msg, local_pids, from_pid, topic) do
    Enum.each(local_pids, fn local_pid ->
      Local.broadcast(local_pid, from_pid, topic, msg)
    end)
  end

  defp pg2_namespace(server_name), do: {:phx, server_name}
end
