defmodule OffBroadway.Splunk.SplunkClient do
  @moduledoc """
  Default Splunk client used by `OffBroadway.Splunk.Producer` to receive data from
  Splunk.
  This module implements the `OffBroadway.Splunk.Client` and `Broadway.Acknowledger`
  behaviours which define callbacks for receiving and acknowledging messages.
  Since Splunk does not have any concept of acknowledging consumed messages, we need
  to keep track of what messages that are consumed ourselves (more on that later).

  The default Splunk client uses the Splunk Web API for receiving messages and is
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
    * `Tesla.Middleware.Query` middleware configured with `query` passed via `opts`.
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
  def receive_status(name, opts) do
    client(opts)
    |> Tesla.get("/services/saved/searches/#{name}/history", query: [output_mode: "json"])
  end

  @impl true
  def receive_messages(sid, _demand, opts) do
    {:ok, version} = Keyword.fetch(opts, :api_version)
    {ack_ref, opts} = Keyword.pop(opts, :ack_ref)

    endpoint =
      case {Keyword.fetch!(opts, :kind), Keyword.fetch!(opts, :endpoint)} do
        {:report, _} -> :results
        {:alert, endpoint} -> endpoint
      end

    client(opts)
    |> Tesla.get("/services/search/#{version}/jobs/#{sid}/#{Atom.to_string(endpoint)}")
    |> log_api_messages()
    |> wrap_received_messages(sid, ack_ref)
  end

  @impl Acknowledger
  def ack(ack_ref, successful, failed) do
    ack_options = :persistent_term.get(ack_ref)

    Stream.concat(
      Stream.filter(successful, &ack?(&1, ack_options, :on_success)),
      Stream.filter(failed, &ack?(&1, ack_options, :on_failure))
    )
    |> Stream.each(&ack_message(&1, ack_options))
    |> Stream.run()
  end

  defp ack?(message, ack_options, option) do
    {_, _, msg_ack_options} = message.acknowledger
    (msg_ack_options[option] || Map.fetch!(ack_options, option)) == :ack
  end

  @impl true
  def ack_message(message, ack_options) do
    :telemetry.execute(
      [:off_broadway_splunk, :receive_messages, :ack],
      %{time: System.system_time(), count: 1},
      %{name: ack_options[:name], receipt: extract_message_receipt(message)}
    )
  end

  @impl Acknowledger
  def configure(_ack_ref, ack_data, options), do: {:ok, Map.merge(ack_data, Map.new(options))}

  defp log_api_messages({:ok, %Tesla.Env{body: %{"messages" => messages}}} = response) do
    log_splunk_message(messages)
    response
  end

  defp log_api_messages(response), do: response

  defp wrap_received_messages(
         {:ok, %Tesla.Env{status: 200, body: %{"results" => messages}}},
         sid,
         ack_ref
       ) do
    Stream.map(messages, fn message ->
      metadata =
        Map.filter(message, fn {key, _val} -> String.starts_with?(key, "_") and key != "_raw" end)
        |> Map.put("sid", sid)

      acknowledger = build_acknowledger(message, ack_ref)
      %Message{data: message, metadata: metadata, acknowledger: acknowledger}
    end)
    |> Enum.to_list()
  end

  defp wrap_received_messages({:ok, %Tesla.Env{status: status_code}}, sid, _ack_ref) do
    Logger.error(
      "Unable to fetch events from Splunk SID \"#{sid}\". " <>
        "Request failed with status code: #{status_code}."
    )

    []
  end

  defp build_acknowledger(message, ack_ref) do
    receipt = %{id: build_splunk_message_id(message)}
    {__MODULE__, ack_ref, %{receipt: receipt}}
  end

  defp log_splunk_message(message) when is_list(message),
    do: Enum.each(message, &log_splunk_message/1)

  defp log_splunk_message(%{"text" => reason, "type" => level}),
    do: Logger.debug(%{"source" => "splunk", "level" => level, "reason" => reason})

  defp extract_message_receipt(%{acknowledger: {_, _, %{receipt: receipt}}}), do: receipt

  # TODO Need a better way to build unique replicable message ids
  defp build_splunk_message_id(%{"_si" => si, "_cd" => cd}) when is_list(si),
    do: "#{Enum.join(si, ";")};#{cd}"

  defp build_splunk_message_id(_), do: nil

  @spec client_option(Keyword.t(), atom()) :: any
  defp client_option(opts, :base_url), do: Keyword.get(opts, :base_url, "")
  defp client_option(opts, :api_token), do: Keyword.get(opts, :api_token, "")
  defp client_option(opts, :query), do: Keyword.get(opts, :query, [])
end
