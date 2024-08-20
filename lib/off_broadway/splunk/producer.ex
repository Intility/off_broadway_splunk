defmodule OffBroadway.Splunk.Producer do
  @moduledoc """
  GenStage Producer for a Splunk Event Stream.
  Broadway producer acts as a consumer for Splunk report or alerts.

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

    * `[:off_broadway_splunk, :receive_jobs, :start]` - Dispatched before fetching jobs
      from Splunk.

      * measurement: `%{time: System.monotonic_time}`
      * metadata: `%{name: string, count: integer}`

    * `[:off_broadway_splunk, :receive_jobs, :stop]` - Dispatched when fetching jobs from Splunk
      is complete.

      * measurement: `%{time: native_time}`
      * metadata: `%{name: string, count: integer}`

    * `[:off_broadway_splunk, :receive_jobs, :exception]` - Dispatched after a failure while fetching
      jobs from Splunk.

      * measurement: `%{duration: native_time}`
      * metadata:

        ```
        %{
          name: string,
          reason: reason,
          stacktrace: stacktrace
        }
        ```

    * `[:off_broadway_splunk, :process_job, :start]` - Dispatched before starting to process
      messages for a job.

      * measurement: `%{time: System.system_time}`
      * metadata: `%{name: string, sid: string}`

    * `[:off_broadway_splunk, :process_job, :stop]` - Dispatched after all messages have been
      processed for a job.

      * measurement: `%{time: System.system_time, processed_events: integer, processed_requests: integer}`
      * metadata: `%{name: string, sid: string}`

    * `[:off_broadway_splunk, :receive_messages, :start]` - Dispatched before receiving
      messages from Splunk.

      * measurement: `%{time: System.monotonic_time}`
      * metadata: `%{name: string, sid: string, demand: integer}`

    * `[:off_broadway_splunk, :receive_messages, :stop]` - Dispatched after messages have been
      received from Splunk and "wrapped".

      * measurement: `%{time: native_time}`
      * metadata:

        ```
        %{
          name: string,
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
          name: string,
          sid: string,
          demand: integer,
          reason: reason,
          stacktrace: stacktrace
        }
        ```

    * `[:off_broadway_splunk, :receive_messages, :ack]` - Dispatched when acking a message.

      * measurement: `%{time: System.system_time, count: 1}`
      * meatadata:

        ```
        %{
          name: string,
          receipt: receipt
        }
        ```
  """

  use GenStage
  alias Broadway.Producer
  alias NimbleOptions.ValidationError
  alias OffBroadway.Splunk.Job

  @behaviour Producer

  @impl true
  def init(opts) do
    client = opts[:splunk_client]
    {:ok, client_opts} = client.init(opts)

    # Use a two-dimensional counter to keep track of counts.
    # First index is count for current job, second is total.
    processed_events_counter = :counters.new(2, [:atomics])
    processed_requests_counter = :counters.new(2, [:atomics])

    {:producer,
     %{
       demand: 0,
       drain: false,
       processed_events: processed_events_counter,
       processed_requests: processed_requests_counter,
       receive_timer: nil,
       receive_interval: opts[:receive_interval],
       refetch_timer: nil,
       refetch_interval: opts[:refetch_interval],
       name: opts[:name],
       jobs: opts[:jobs],
       current_job: nil,
       first_fetch: true,
       completed_jobs: MapSet.new(),
       queue: :queue.new(),
       splunk_client: {client, client_opts},
       broadway: opts[:broadway][:name],
       shutdown_timeout: opts[:shutdown_timeout]
     }}
  end

  @impl true
  def prepare_for_start(_module, broadway_opts) do
    {producer_module, client_opts} = broadway_opts[:producer][:module]
    client_opts = preprocess_options(client_opts)

    case NimbleOptions.validate(client_opts, OffBroadway.Splunk.Options.definition()) do
      {:error, error} ->
        raise ArgumentError, format_error(error)

      {:ok, opts} ->
        ack_ref = broadway_opts[:name]

        :persistent_term.put(ack_ref, %{
          name: opts[:name],
          config: opts[:config],
          on_success: opts[:on_success],
          on_failure: opts[:on_failure]
        })

        with_default_opts = put_in(broadway_opts, [:producer, :module], {producer_module, opts})

        {[], with_default_opts}
    end
  end

  # NOTE Remove next major release when :only_new and :only_latest are removed.
  defp preprocess_options(opts) do
    Enum.reduce(opts, [], fn
      {:only_new, true}, acc -> Keyword.put_new(acc, :jobs, :new)
      {:only_latest, true}, acc -> Keyword.put_new(acc, :jobs, :latest)
      {key, value}, acc -> Keyword.put(acc, key, value)
    end)
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
  def handle_demand(incoming_demand, %{demand: demand, receive_timer: timer} = state) do
    timer && Process.cancel_timer(timer)
    handle_receive_messages(%{state | demand: demand + incoming_demand, receive_timer: nil})
  end

  @impl true
  def handle_info(:receive_jobs, %{refetch_timer: timer} = state) do
    timer && Process.cancel_timer(timer)
    handle_receive_jobs(%{state | refetch_timer: nil})
  end

  def handle_info(:receive_messages, %{receive_timer: timer} = state) do
    timer && Process.cancel_timer(timer)
    handle_receive_messages(%{state | receive_timer: nil})
  end

  def handle_info(:next_job, %{receive_timer: timer} = state) do
    timer && Process.cancel_timer(timer)
    handle_next_job(%{state | receive_timer: nil})
  end

  def handle_info(
        :shutdown_broadway,
        %{
          receive_timer: receive_timer,
          refetch_timer: refetch_timer,
          shutdown_timeout: timeout,
          broadway: broadway
        } = state
      ) do
    receive_timer && Process.cancel_timer(receive_timer)
    refetch_timer && Process.cancel_timer(refetch_timer)
    Broadway.stop(broadway, :normal, timeout)
    {:noreply, [], %{state | receive_timer: nil}}
  end

  def handle_info(_, state), do: {:noreply, [], state}

  @impl Producer
  def prepare_for_draining(%{receive_timer: receive_timer, refetch_timer: refetch_timer} = state) do
    receive_timer && Process.cancel_timer(receive_timer)
    refetch_timer && Process.cancel_timer(refetch_timer)
    {:noreply, [], %{state | drain: true, receive_timer: nil, refetch_timer: nil}}
  end

  @spec handle_receive_jobs(state :: map()) :: {:noreply, [], new_state :: map()}
  defp handle_receive_jobs(%{refetch_timer: nil} = state) do
    new_state =
      receive_jobs_from_splunk(state)
      |> update_queue_from_response(state)

    case new_state do
      %{current_job: nil, queue: {[], []}} ->
        {:noreply, [],
         %{new_state | refetch_timer: schedule_receive_jobs(state.refetch_interval)}}

      %{current_job: nil, queue: _queue} ->
        {:noreply, [],
         %{
           new_state
           | receive_timer: schedule_next_job(0),
             refetch_timer: schedule_receive_jobs(state.refetch_interval)
         }}
    end
  end

  defp handle_next_job(
         %{current_job: current, completed_jobs: completed, receive_timer: nil} = state
       ) do
    unless is_nil(current) do
      :telemetry.execute(
        [:off_broadway_splunk, :process_job, :stop],
        %{
          time: System.system_time(),
          processed_events: :counters.get(state.processed_events, 1),
          processed_requests: :counters.get(state.processed_requests, 1)
        },
        %{name: state.name, sid: current.name}
      )
    end

    case :queue.out(state.queue) do
      {{:value, job}, new_queue} ->
        :ok = :counters.put(state.processed_events, 1, 0)
        :ok = :counters.put(state.processed_requests, 1, 0)

        :telemetry.execute(
          [:off_broadway_splunk, :process_job, :start],
          %{time: System.system_time()},
          %{name: state.name, sid: job.name}
        )

        {:noreply, [],
         %{
           state
           | current_job: job,
             queue: new_queue,
             completed_jobs: MapSet.put(completed, current),
             receive_timer: schedule_receive_messages(0)
         }}

      {:empty, new_queue} ->
        {:noreply, [],
         %{
           state
           | current_job: nil,
             queue: new_queue,
             completed_jobs: MapSet.put(completed, current),
             refetch_timer: schedule_receive_jobs(state.refetch_interval)
         }}
    end
  end

  defp handle_receive_messages(%{drain: true} = state), do: {:noreply, [], state}

  defp handle_receive_messages(
         %{
           current_job: nil,
           refetch_timer: nil,
           receive_timer: nil,
           queue: {[], []}
         } = state
       ) do
    with {:ok, response} <- receive_jobs_from_splunk(state),
         new_state <- update_queue_from_response({:ok, response}, state) do
      {:noreply, [], %{new_state | receive_timer: schedule_receive_messages(0)}}
    else
      {:error, _reason} ->
        {:noreply, [],
         %{state | receive_timer: schedule_receive_messages(state.receive_interval)}}
    end
  end

  defp handle_receive_messages(
         %{
           receive_timer: nil,
           current_job: %Job{},
           demand: demand,
           splunk_client: {_, client_opts}
         } = state
       )
       when demand > 0 do
    {messages, new_state} = receive_messages_from_splunk(state, demand)
    new_demand = demand - length(messages)
    max_events = client_opts[:max_events]
    total_events = :counters.get(state.processed_events, 2)

    receive_timer =
      case {total_events, messages, new_state} do
        {^max_events, _messages, _state} -> schedule_shutdown()
        {_total_events, [], %{receive_interval: interval}} -> schedule_next_job(interval)
        {_total_events, _messages, _state} -> schedule_receive_messages(0)
      end

    {:noreply, messages, %{new_state | demand: new_demand, receive_timer: receive_timer}}
  end

  defp handle_receive_messages(%{current_job: nil, receive_timer: nil} = state) do
    case :queue.peek(state.queue) do
      {:value, _} ->
        {:noreply, [], %{state | receive_timer: schedule_next_job(0)}}

      :empty ->
        {:noreply, [],
         %{state | receive_timer: schedule_receive_messages(state.receive_interval)}}
    end
  end

  defp handle_receive_messages(%{receive_timer: nil} = state) do
    {:noreply, [], %{state | receive_timer: schedule_receive_messages(state.receive_interval)}}
  end

  @spec receive_jobs_from_splunk(state :: map()) :: {:ok, Tesla.Env.t()}
  defp receive_jobs_from_splunk(%{name: name, splunk_client: {client, client_opts}}) do
    metadata = %{name: name, count: 0}

    :telemetry.span(
      [:off_broadway_splunk, :receive_jobs],
      metadata,
      fn ->
        case client.receive_status(name, client_opts) do
          {:ok, %{status: 200, body: %{"entry" => jobs}}} = response ->
            {response, %{metadata | count: length(jobs)}}

          {:ok, %{status: _status}} = response ->
            {response, metadata}

          {:error, reason} ->
            {{:error, reason}, metadata}
        end
      end
    )
  end

  @spec receive_messages_from_splunk(state :: map(), demand :: non_neg_integer()) ::
          {messages :: list(), state :: map()}
  defp receive_messages_from_splunk(
         %{name: name, current_job: job, splunk_client: {client, client_opts}} = state,
         demand
       ) do
    metadata = %{name: name, sid: job.name, demand: demand}
    count = calculate_count(client_opts, demand, :counters.get(state.processed_events, 1))

    client_opts =
      Keyword.put(client_opts, :ack_ref, state.broadway)
      |> Keyword.put(:query,
        output_mode: "json",
        count: count,
        offset: :counters.get(state.processed_events, 1)
      )

    case count do
      0 ->
        {[], state}

      _ ->
        messages =
          :telemetry.span(
            [:off_broadway_splunk, :receive_messages],
            metadata,
            fn ->
              with messages <- client.receive_messages(job.name, demand, client_opts),
                   count <- length(messages),
                   :ok <- :counters.add(state.processed_events, 1, count),
                   :ok <- :counters.add(state.processed_events, 2, count),
                   :ok <- :counters.add(state.processed_requests, 1, 1),
                   :ok <- :counters.add(state.processed_requests, 2, 1) do
                {messages, Map.put(metadata, :received, count)}
              end
            end
          )

        {messages, state}
    end
  end

  @spec update_queue_from_response(
          response :: {:ok, Tesla.Env.t()} | {:error, any()},
          state :: map()
        ) ::
          new_state :: map()
  defp update_queue_from_response({:ok, %{status: 200, body: %{"entry" => jobs}}}, state) do
    jobs =
      Enum.map(jobs, &merge_non_nil_fields(Job.new(&1), Job.new(Map.get(&1, "content"))))
      |> Enum.reject(& &1.is_zombie)
      |> Enum.filter(& &1.is_done)
      |> Enum.sort_by(& &1.published, {:asc, DateTime})

    # If the `:jobs` option is set to `:new`, add all existing jobs to
    # "completed jobs", and wait for new to arrive.
    completed_jobs =
      if state.first_fetch && state.jobs == :new do
        Enum.reduce(jobs, state.completed_jobs, fn job, acc ->
          MapSet.put(acc, job)
        end)
      else
        state.completed_jobs
      end

    new_queue =
      :queue.fold(
        fn job, acc ->
          with false <- job == state.current_job,
               false <- :queue.member(job, acc),
               false <- MapSet.member?(completed_jobs, job) do
            :queue.in(job, acc)
          else
            true -> acc
          end
        end,
        state.queue,
        :queue.from_list(only_latest?(jobs, state.jobs))
      )

    %{state | queue: new_queue, completed_jobs: completed_jobs, first_fetch: false}
  end

  defp update_queue_from_response({:ok, _response}, state), do: state
  defp update_queue_from_response({:error, _reason}, state), do: state

  @spec only_latest?(list :: list(), flag :: atom()) :: list()
  defp only_latest?(list, :latest), do: Enum.take(list, -1)
  defp only_latest?(list, _), do: list

  @spec merge_non_nil_fields(map_a :: map(), map_b :: map()) :: map()
  defp merge_non_nil_fields(map_a, map_b) do
    Map.merge(map_a, map_b, fn
      _key, old_value, new_value when is_nil(new_value) -> old_value
      _key, _old_value, new_value -> new_value
    end)
  end

  @spec calculate_count(
          client_opts :: map(),
          demand :: non_neg_integer(),
          processed_events :: non_neg_integer()
        ) :: non_neg_integer()
  defp calculate_count(client_opts, demand, processed_events) do
    case client_opts[:max_events] do
      nil ->
        demand

      max_events ->
        capacity = max_events - processed_events
        min(demand - (demand - capacity), demand)
    end
  end

  @spec schedule_next_job(interval :: non_neg_integer()) :: reference()
  defp schedule_next_job(interval),
    do: Process.send_after(self(), :next_job, interval)

  @spec schedule_receive_jobs(interval :: non_neg_integer()) :: reference()
  defp schedule_receive_jobs(interval),
    do: Process.send_after(self(), :receive_jobs, interval)

  @spec schedule_receive_messages(interval :: non_neg_integer()) :: reference()
  defp schedule_receive_messages(interval),
    do: Process.send_after(self(), :receive_messages, interval)

  @spec schedule_shutdown() :: reference()
  defp schedule_shutdown,
    do: Process.send_after(self(), :shutdown_broadway, 0)
end
