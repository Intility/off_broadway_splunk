defmodule OffBroadwaySplunk.Producer do
  @moduledoc """
  GenStage Producer for a Splunk Event Stream.

  Broadway producer acts as a consumer for the specified Splunk SID.

  ## Producer Options

    * `:sid`  - Required. The Splunk SID to consume events for.
    * `:receive_interval` - Optional. The duration (in milliseconds) for which the producer waits
      before making a request for more events if there are no events in the stream. Defaults to `1000`.
  """

  # use GenStage
  # alias Broadway.Producer
  # alias Broadway.Message
end
