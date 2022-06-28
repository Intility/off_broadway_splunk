defmodule OffBroadway.Splunk.ProducerTest do
  use ExUnit.Case, async: true

  alias Broadway.Message
  import ExUnit.CaptureLog

  defmodule MessageServer do
    def start_link, do: Agent.start_link(fn -> [] end)

    def push_messages(server, messages),
      do: Agent.update(server, fn queue -> queue ++ messages end)

    def take_messages(server, amount),
      do: Agent.get_and_update(server, &Enum.split(&1, amount))
  end
end
