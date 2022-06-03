defmodule OffBroadway.SplunkTest do
  use ExUnit.Case, async: true

  alias OffBroadway.Splunk

  test "via_tuple/1 returns a via-tuple for the registry" do
    assert {:via, Registry, {OffBroadway.Splunk.Registry, "a name"}} = Splunk.via_tuple("a name")
  end
end
