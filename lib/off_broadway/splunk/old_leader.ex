defmodule OffBroadway.Splunk.OldLeader do
  @moduledoc """
  The `OffBroadway.Splunk.Leader` module is responsible to poll Splunk
  for status on a SID and notify the `OffBroadway.Splunk.Producer` when
  Splunk is ready to deliver messages for given SID.
  """

  defmodule State do
    defstruct [
      :done_progress,
      :event_count,
      :is_done,
      :is_zombie,
      :is_scheduled,
      :on_success,
      :on_failure,
      :broadway,
      :published,
      :sid,
      :name,
      :splunk_client
    ]

    @type t :: %__MODULE__{
            done_progress: nil | integer() | float(),
            event_count: nil | integer(),
            is_done: boolean(),
            is_zombie: boolean(),
            is_scheduled: boolean(),
            on_success: atom(),
            on_failure: atom(),
            broadway: atom(),
            published: String.t(),
            sid: String.t(),
            name: String.t(),
            splunk_client: tuple()
          }
    use ExConstructor
  end

  use GenServer
  require Logger

  alias Decimal, as: D

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    client = opts[:splunk_client]
    {:ok, client_opts} = client.init(opts)

    state = %{
      progress: 0,
      is_done: false,
      on_success: opts[:on_success],
      on_failure: opts[:on_failure],
      splunk_client: {client, client_opts}
    }

    Process.send_after(self(), :receive_job_status, 0)
    {:ok, State.new(opts) |> Map.merge(state)}
  end

  @impl true
  def handle_info(:receive_job_status, %State{is_zombie: true} = state) do
    Logger.error("Job is in zombie state - Shutting down")
    {:stop, :normal, state}
  end

  def handle_info(:receive_job_status, %{sid: sid, is_done: false} = state) do
    case receive_job_status(state) do
      {:ok, %{status: 200} = response} ->
        state = update_state_from_response(state, response)
        receive_interval = calculate_receive_interval(state)

        if receive_interval > 0 do
          Logger.info(
            "SID \"#{sid}\" is #{Float.ceil(state.done_progress * 100, 2)}% complete, " <>
              "rescheduling update in #{receive_interval} seconds"
          )
        end

        Process.send_after(self(), :receive_job_status, receive_interval * 1000)
        {:noreply, state}

      {:ok, %{status: 404}} ->
        Logger.error("SID \"#{sid}\" does not exist - Shutting down")
        {:stop, :normal, state}

      reason ->
        Logger.error("SID \"#{sid}\" failed with reason #{inspect(reason)} - Shutting down")
        {:stop, :normal, state}
    end
  end

  def handle_info(
        :receive_job_status,
        %State{sid: sid, is_done: true, broadway: broadway, splunk_client: {_client, client_opts}} =
          state
      ) do
    Logger.info("Splunk is done processing SID \"#{sid}\" - Ready to consume events")

    # NOTE In case of :report producers, we're changing the provided SID with the one
    # received from the Splunk API here. This is because when consuming reports, the
    # given report name is just a placeholder, while the actual SID for the job changes
    # for each time the report runs.
    job_sid =
      case Keyword.fetch!(client_opts, :kind) do
        :report -> state.name
        :alert -> state.sid
      end

    :persistent_term.put(job_sid, %{
      sid: job_sid,
      config: client_opts,
      on_success: state.on_success,
      on_failure: state.on_failure
    })

    Broadway.producer_names(broadway)
    |> Enum.random()
    |> GenStage.cast({:receive_messages_ready, total_events: state.event_count, sid: job_sid})

    {:noreply, state}
  end

  @spec receive_job_status(state :: State.t()) :: Tesla.Env.t()
  defp receive_job_status(
         %{sid: sid, done_progress: progress, splunk_client: {client, client_opts}} = state
       ) do
    metadata = %{sid: sid, progress: progress}

    :telemetry.span(
      [:off_broadway_splunk, :job_status],
      metadata,
      fn ->
        {:ok, %{status: 200} = response} = env = client.receive_status(sid, client_opts)
        state = update_state_from_response(state, response)
        {env, %{metadata | progress: state.done_progress}}
      end
    )
  end

  @spec update_state_from_response(state :: State.t(), response :: Tesla.Env.t()) :: State.t()
  defp update_state_from_response(state, %{status: 200} = response) do
    %{"content" => content} = entry = latest_entry_from_response(response)

    merge_non_nil_state_fields(State.new(entry), state)
    |> merge_non_nil_state_fields(State.new(content))
  end

  @spec latest_entry_from_response(response :: Tesla.Env.t()) :: map()
  defp latest_entry_from_response(%{body: %{"entry" => content}}) when is_list(content) do
    Enum.map(content, fn %{"published" => published} = entry ->
      {:ok, datetime, _offset} = DateTime.from_iso8601(published)
      Map.put(entry, "published", datetime)
    end)
    |> Enum.max_by(&Map.fetch!(&1, "published"), DateTime)
  end

  @spec merge_non_nil_state_fields(state_a :: State.t(), state_b :: State.t()) :: State.t()
  defp merge_non_nil_state_fields(state_a, state_b) do
    Map.merge(state_a, state_b, fn
      _key, old_value, new_value when is_nil(new_value) -> old_value
      _key, _old_value, new_value -> new_value
    end)
  end

  # Calculates the next receive interval for fetching the job meta data.
  #
  # Algorithm is as follows:
  #  T = time difference since job was started
  #  P = job progress (normalized value)
  #
  #  seconds until next check = (T * (1 / P)) - T
  #
  @spec calculate_receive_interval(state :: State.t()) :: integer()
  defp calculate_receive_interval(%State{is_done: true}), do: 0
  defp calculate_receive_interval(%State{done_progress: 1}), do: 0

  defp calculate_receive_interval(%State{published: published, done_progress: progress}) do
    with {:ok, published_dt, _offset} = DateTime.from_iso8601(published),
         diff <- DateTime.diff(DateTime.utc_now(), published_dt, :second) do
      D.mult(diff, D.div(1, D.from_float(progress)))
      |> D.sub(diff)
      |> D.to_float()
      |> ceil()
    end
  end
end
