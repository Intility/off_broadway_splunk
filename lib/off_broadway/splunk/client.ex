defmodule OffBroadway.Splunk.Client do
  @moduledoc """
  A generic behaviour for implementing Splunk clients for
  `OffBroadway.Splunk.Producer`.

  This module defines callbacks to normalize options and receive items
  for a Splunk SID (Search ID).

  Modules that implements this behaviour should be passed as the
  `:splunk_client` option from `OffBroadway.Splunk.Producer`.
  """

  alias Broadway.Message

  @type client :: Tesla.Client.t()
  @type status :: Tesla.Env.t()
  @type messages :: [Message.t()]

  @callback init(opts :: any) :: {:ok, normalized_opts :: any} | {:error, reason :: binary}
  @callback client(opts :: keyword) :: client
  @callback receive_status(client :: client, sid :: binary) :: status
  @callback receive_messages(client :: client, sid :: binary, demand :: pos_integer, opts :: any) ::
              messages
end
