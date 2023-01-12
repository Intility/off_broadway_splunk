defmodule OffBroadway.Splunk.Producer do
  @moduledoc """
  GenStage Producer for a Splunk Event Stream.
  Broadway producer acts as a consumer for the specified Splunk SID.

  ## Producer Options

  #{NimbleOptions.docs(OffBroadway.Splunk.Options.definition())}

  ## Acknowledgements

  You can use the `on_success` and `on_failure` options to control how messages are
  acknowledged. You can set these options when starting the Splunk producer or change
  them for each message through `Broadway.Message.configure_ack/2`. By default, successful
  messages are acked (`:ack`) and failed messages are not (`:noop`).

  The possible values for `:on_success` and `:on_failure` are:

    * `:ack` - acknowledge the message. Splunk does not have any concept of acking messages,
      because we are just consuming messages from a web api endpoint.
      For now we are just executing a `:telemetry` event for acked messages.

    * `:noop` - do not acknowledge the message. No action are taken.

  ## Telemetry

  This library exposes the following telemetry events:

    * `[:off_broadway_splunk, :job_status, :start]` - Dispatched before polling SID status
      from Splunk.

      * measurement: `%{time: System.monotonic_time}`
      * metadata: `%{sid: string, progress: integer}`

    * `[:off_broadway_splunk, :job_status, :stop]` - Dispatched when polling SID status from Splunk
      is complete.

      * measurement: `%{time: native_time}`
      * metadata: %{sid: string, progress: integer}

    * `[:off_broadway_splunk, :job_status, :exception]` - Dispatched after a failure while polling
      SID status from Splunk.

      * measurement: `%{duration: native_time}`
      * metadata:

        ```
        %{
          sid: string,
          kind: kind,
          reason: reason,
          stacktrace: stacktrace
        }
        ```

    * `[:off_broadway_splunk, :receive_messages, :start]` - Dispatched before receiving
      messages from Splunk (`c:receive_messages/2`)

      * measurement: `%{time: System.monotonic_time}`
      * metadata: `%{sid: string, demand: integer}`

    * `[:off_broadway_splunk, :receive_messages, :stop]` - Dispatched after messages have been
      received from Splunk and "wrapped".

      * measurement: `%{time: native_time}`
      * metadata:

        ```
        %{
          sid: string,
          received: integer,
          demand: integer
        }
        ```

    * `[:off_broadway_splunk, :receive_messages, :exception]` - Dispatched after a failure while
      receiving messages from Splunk.

      * measurement: `%{duration: native_time}`
      * metadata:

        ```
        %{
          sid: string,
          demand: integer,
          kind: kind,
          reason: reason,
          stacktrace: stacktrace
        }
        ```

    * `[:off_broadway_splunk, :receive_messages, :ack]` - Dispatched when acking a message.

      * measurement: `%{time: System.system_time, count: 1}`
      * meatadata:

        ```
        %{
          sid: string,
          receipt: receipt
        }
        ```

  """

  use GenStage
  alias Broadway.Producer
  alias NimbleOptions.ValidationError
  alias OffBroadway.Splunk.Leader

  require Logger

  @behaviour Producer

  @impl true
  def init(opts) do
    client = opts[:splunk_client]
    {:ok, client_opts} = client.init(opts)

    {:producer,
     %{
       demand: 0,
       total_events: 0,
       processed_events: 0,
       processed_requests: 0,
       receive_timer: nil,
       receive_interval: opts[:receive_interval],
       ready: false,
       sid: opts[:sid],
       splunk_client: {client, client_opts}
     }}
  end

  @impl true
  def prepare_for_start(_module, broadway_opts) do
    {producer_module, client_opts} = broadway_opts[:producer][:module]

    case NimbleOptions.validate(client_opts, OffBroadway.Splunk.Options.definition()) do
      {:error, error} ->
        raise ArgumentError, format_error(error)

      {:ok, opts} ->
        :persistent_term.put(opts[:sid], %{
          sid: opts[:sid],
          config: opts[:config],
          on_success: opts[:on_success],
          on_failure: opts[:on_failure]
        })

        with_default_opts = put_in(broadway_opts, [:producer, :module], {producer_module, opts})

        children = [
          {Leader, Keyword.merge(opts, broadway: with_default_opts[:name])}
        ]

        {children, with_default_opts}
    end
  end

  defp format_error(%ValidationError{keys_path: [], message: message}) do
    "invalid configuration given to OffBroadway.Splunk.Producer.prepare_for_start/2, " <>
      message
  end

  defp format_error(%ValidationError{keys_path: keys_path, message: message}) do
    "invalid configuration given to OffBroadway.Splunk.Producer.prepare_for_start/2 for key #{inspect(keys_path)}, " <>
      message
  end

  @impl true
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    handle_receive_messages(%{state | demand: demand + incoming_demand})
  end

  @impl true
  def handle_info(:receive_messages, %{receive_timer: nil} = state), do: {:noreply, [], state}

  def handle_info(:receive_messages, state),
    do: handle_receive_messages(%{state | receive_timer: nil})

  def handle_info(_, state), do: {:noreply, [], state}

  @impl true
  # Callback function used by `OffBroadway.Splunk.Leader` to notify that
  # Splunk API is ready to deliver messages.
  def handle_cast({:receive_messages_ready, total_events: event_count}, state),
    do: handle_receive_messages(%{state | total_events: event_count, ready: true})

  @impl Producer
  def prepare_for_draining(%{receive_timer: receive_timer} = state) do
    receive_timer && Process.cancel_timer(receive_timer)
    {:noreply, [], %{state | receive_timer: nil}}
  end

  defp handle_receive_messages(
         %{receive_timer: nil, ready: true, demand: demand, processed_events: processed_events} =
           state
       )
       when demand > 0 do
    {messages, state} = receive_messages_from_splunk(state, demand)
    new_demand = demand - length(messages)

    receive_timer =
      case {messages, state.demand} do
        {[], _} -> schedule_receive_messages(state.receive_interval)
        {_, 0} -> nil
        _ -> schedule_receive_messages(0)
      end

    {:noreply, messages,
     %{
       state
       | demand: new_demand,
         processed_events: processed_events + length(messages),
         receive_timer: receive_timer
     }}
  end

  defp handle_receive_messages(state), do: {:noreply, [], state}

  defp receive_messages_from_splunk(
         %{sid: sid, splunk_client: {client, client_opts}} = state,
         total_demand
       ) do
    metadata = %{sid: sid, demand: total_demand}

    client_opts =
      Keyword.put(client_opts, :query,
        output_mode: "json",
        count: total_demand,
        offset: calculate_offset(state)
      )

    {:telemetry.span(
       [:off_broadway_splunk, :receive_messages],
       metadata,
       fn ->
         messages = client.receive_messages(sid, total_demand, client_opts)
         {messages, Map.put(metadata, :received, length(messages))}
       end
     ), %{state | processed_requests: state.processed_requests + 1}}
  end

  defp calculate_offset(%{splunk_client: {_, client_opts}, processed_requests: 0}),
    do: client_opts[:offset]

  defp calculate_offset(%{splunk_client: {_, client_opts}, processed_events: processed_events}) do
    case {client_opts[:offset], processed_events} do
      {offset, processed_events} when offset < 0 -> -abs(abs(offset) + processed_events)
      {offset, processed_events} when offset >= 0 -> offset + processed_events
    end
  end

  defp schedule_receive_messages(interval),
    do: Process.send_after(self(), :receive_messages, interval)
end
