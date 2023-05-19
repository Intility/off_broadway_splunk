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

  @type messages :: [Message.t()]

  @callback init(opts :: any) :: {:ok, normalized_opts :: any} | {:error, reason :: binary}
  @callback ack_message(message :: Message.t(), ack_options :: any) :: any
  @callback receive_status(name :: binary, opts :: any) ::
              {:ok, response :: any} | {:error, reason :: any}
  @callback receive_messages(sid :: binary, demand :: pos_integer, opts :: any) :: messages

  @optional_callbacks ack_message: 2
end
