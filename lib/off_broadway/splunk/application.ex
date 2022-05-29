defmodule OffBroadway.Splunk.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: OffBroadway.Splunk.Registry},
      {Task.Supervisor, name: OffBroadway.Splunk.TaskSupervisor}
    ]

    opts = [strategy: :one_for_one, name: OffBroadway.Splunk.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
