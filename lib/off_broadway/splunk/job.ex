defmodule OffBroadway.Splunk.Job do
  @moduledoc false

  defstruct [
    :name,
    :published,
    :is_done,
    :is_zombie,
    :is_scheduled
  ]

  @type t :: %__MODULE__{
          name: String.t(),
          published: String.t(),
          is_done: boolean(),
          is_zombie: boolean(),
          is_scheduled: boolean()
        }

  use ExConstructor

  def new(map_or_kwlist, opts \\ []) do
    case super(map_or_kwlist, opts) do
      %{published: published} = struct when is_binary(published) ->
        {:ok, datetime, _offset} = DateTime.from_iso8601(published)
        %{struct | published: datetime}

      struct ->
        struct
    end
  end
end
