defmodule OffBroadway.Splunk.Leader do
  @moduledoc """
  Leader process..
  """

  defmodule State do
    defstruct [
      :done_progress,
      :event_count,
      :is_done,
      :is_zombie,
      :broadway,
      :published,
      :sid,
      :splunk_client
    ]

    @type t :: %__MODULE__{
            done_progress: nil | Integer.t() | Float.t(),
            event_count: Integer.t(),
            is_done: boolean(),
            is_zombie: boolean(),
            broadway: Atom.t(),
            published: String.t(),
            sid: String.t(),
            splunk_client: Tuple.t()
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
      is_done: false,
      splunk_client: {client, client_opts}
    }

    Process.send_after(self(), :receive_sid_status, 0)
    {:ok, State.new(opts) |> Map.merge(state)}
  end

  @impl true
  def handle_info(:receive_sid_status, %State{is_zombie: true} = state) do
    Logger.error("Job is in zombie state - Shutting down")
    {:stop, :normal, state}
  end

  def handle_info(
        :receive_sid_status,
        %{sid: sid, is_done: false, splunk_client: {module, client_opts}} = state
      ) do
    # TODO - Add telemetry!
    with client <- module.client(client_opts),
         {:ok, %{status: 200} = response} <- module.receive_status(client, sid) do
      state = update_state_from_response(state, response)
      receive_interval = calculate_receive_interval(state)

      unless state.is_done do
        Logger.info(
          "SID #{sid} is #{Float.ceil(state.done_progress * 100, 2)}% complete, " <>
            "rescheduling update in #{receive_interval} seconds"
        )
      end

      Process.send_after(self(), :receive_sid_status, receive_interval * 1000)
      {:noreply, state}
    else
      {:ok, %{status: 404}} ->
        Logger.error("SID #{sid} does not exist - Shutting down")
        {:stop, :normal, state}

      reason ->
        Logger.error("SID #{sid} failed with reason #{inspect(reason)} - Shutting down")
        {:stop, :normal, state}
    end
  end

  def handle_info(
        :receive_sid_status,
        %State{sid: sid, is_done: true, broadway: broadway} = state
      ) do
    Logger.info("Splunk is done processing SID #{sid} - Ready to consume events")

    Broadway.producer_names(broadway)
    |> Enum.random()
    |> GenStage.cast({:receive_messages_ready, total_events: state.event_count})

    {:noreply, state}
  end

  @spec update_state_from_response(State.t(), Tesla.Env.t()) :: State.t()
  defp update_state_from_response(state, %{
         status: 200,
         body: %{"entry" => [%{"content" => content} = entry | _rest]}
       }) do
    merge_non_nil_state_fields(State.new(entry), state)
    |> merge_non_nil_state_fields(State.new(content))
  end

  @spec merge_non_nil_state_fields(State.t(), State.t()) :: State.t()
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
  @spec calculate_receive_interval(State.t()) :: Integer.t()
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
