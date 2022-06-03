defmodule OffBroadway.Splunk do
  @moduledoc false

  @doc """
  Returns a via-tuple for registering processes in the `OffBroadway.Splunk.Registry`
  registry.
  """
  @spec via_tuple(name :: term()) :: Tuple.t()
  def via_tuple(name), do: {:via, Registry, {OffBroadway.Splunk.Registry, name}}
end
