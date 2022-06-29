defmodule OffBroadway.Splunk.SplunkClient do
  @moduledoc """
  Default Splunk client used by `OffBroadway.Splunk.Producer` to receive data from
  Splunk.
  This module implements the `OffBroadway.Splunk.Client` and `Broadway.Acknowledger`
  behaviours which define callbacks for receiving and acknowledging events.
  Since Splunk does not have any concept of acknowledging consumed events, we need
  to keep track of what events that are consumed ourselves (more on that later).

  The default Splunk client uses the Splunk Web API for receiving events and is
  implemented using the [Tesla](https://hexdocs.pm/tesla/readme.html) library. Tesla
  is a HTTP client abstraction library which let's us easily select from a range of
  HTTP adapters. Please see the Tesla [documentation](https://hexdocs.pm/tesla/readme.html#adapters)
  for more information.
  """

  alias Broadway.{Message, Acknowledger}
  require Logger

  @behaviour Acknowledger
  @behaviour OffBroadway.Splunk.Client

  @impl true
  def init(opts) do
    {:ok, opts |> prepare_cfg(Application.get_env(:off_broadway_splunk, :splunk_client) || [])}
  end

  @spec prepare_cfg(opts :: Keyword.t(), env :: Keyword.t()) :: Keyword.t()
  defp prepare_cfg(opts, env), do: Keyword.merge(env, Keyword.get(opts, :config))

  @doc """
  Returns a `Tesla.Client` configured with middleware.

    * `Tesla.Middleware.BaseUrl` middleware configured with `base_url` passed via `opts`.
    * `Tesla.Middleware.BearerAuth` middleware configured with `api_token` passed via `opts`.
    * `Tesla.Middleware.JSON` middleware configured with `Jason` engine.

  """
  @spec client(opts :: Keyword.t()) :: Tesla.Client.t()
  def client(opts) do
    middleware = [
      {Tesla.Middleware.BaseUrl, client_option(opts, :base_url)},
      {Tesla.Middleware.BearerAuth, token: client_option(opts, :api_token)},
      {Tesla.Middleware.Query, client_option(opts, :query)},
      {Tesla.Middleware.JSON, engine: Jason}
    ]

    Tesla.client(middleware)
  end

  @impl true
  def receive_status(sid, opts),
    do: client(opts) |> Tesla.get("/services/search/jobs/#{sid}", query: [output_mode: "json"])

  @impl true
  def receive_messages(sid, _demand, opts) do
    client(opts)
    |> Tesla.get("/services/search/jobs/#{sid}/results")
    |> wrap_received_messages(sid)
  end

  @impl Acknowledger
  def ack(ack_ref, successful, failed) do
    ack_options = :persistent_term.get(ack_ref)

    messages =
      Enum.filter(successful, &ack?(&1, ack_options, :on_success)) ++
        Enum.filter(failed, &ack?(&1, ack_options, :on_failure))

    Enum.each(messages, &ack_message(&1, ack_options))
  end

  defp ack?(message, ack_options, option) do
    {_, _, msg_ack_options} = message.acknowledger
    (msg_ack_options[option] || Map.fetch!(ack_options, option)) == :ack
  end

  defp ack_message(_message, _ack_options) do
    IO.puts("acking message!")
  end

  @impl Acknowledger
  def configure(_ack_ref, ack_data, options), do: {:ok, Map.merge(ack_data, Map.new(options))}

  defp wrap_received_messages(
         {:ok, %Tesla.Env{status: 200, body: %{"results" => messages}}},
         ack_ref
       ) do
    # TODO - Add proper metadata
    Enum.map(messages, fn message ->
      acknowledger = build_acknowledger(message, ack_ref)
      %Message{data: message, metadata: %{foo: "bar"}, acknowledger: acknowledger}
    end)
  end

  defp wrap_received_messages({:ok, %Tesla.Env{status: status_code} = env}, ack_ref) do
    # TODO - Better error handling
    Logger.error(
      "Unable to fetch events from Splunk SID #{ack_ref}. Status code: #{status_code}."
    )
  end

  defp build_acknowledger(_message, ack_ref) do
    # TODO - Add message receipt (id + handle) or something
    {__MODULE__, ack_ref, %{receipt: "receipt here!"}}
  end

  @spec client_option(Keyword.t(), Atom.t()) :: any
  defp client_option(opts, :base_url), do: Keyword.get(opts, :base_url, "")
  defp client_option(opts, :api_token), do: Keyword.get(opts, :api_token, "")
  defp client_option(opts, :query), do: Keyword.get(opts, :query, [])
end
