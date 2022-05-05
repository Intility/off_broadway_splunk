defmodule OffBroadwaySplunk.JobMonitor do
  @moduledoc """
  GenServer responsible for monitoring a Splunk SID search job.

  Whenever Splunk starts a search job, it will be given a unique SID (Search ID).
  As events cannot always be served instantaneous, Splunk will start a background
  job fetching events matching the search criteria. We can query the progress of
  the background job by referring it with the SID, and once it is completed we can
  start downloading the actual events.
  """

  defmodule State do
    @derive Jason.Encoder
    defstruct [
      :author,
      :done_progress,
      :event_count,
      :is_done,
      :is_zombie,
      :published,
      :results,
      :sid
    ]

    @type t :: %__MODULE__{
            author: String.t(),
            done_progress: nil | Integer.t() | Float.t(),
            event_count: Integer.t() | String.t(),
            is_done: boolean(),
            is_zombie: boolean(),
            published: String.t(),
            results: String.t(),
            sid: String.t()
          }
    use ExConstructor
  end

  use GenServer
  require Logger

  alias Decimal, as: D
  alias OffBroadwaySplunk.ApiClient

  def start_link(%{sid: sid} = params),
    do: GenServer.start_link(__MODULE__, params, name: :"#{__MODULE__}-#{sid}")

  @impl true
  def init(%{sid: sid}) do
    Process.send_after(self(), :tick, 0)
    {:ok, %State{sid: sid, is_done: false}}
  end

  @impl true
  def handle_info(:tick, %State{is_zombie: true} = state) do
    Logger.error("Job is in zombie state - Shutting down")
    {:stop, :normal, state}
  end

  def handle_info(:tick, %State{sid: sid, is_done: false} = state) do
    with client <- ApiClient.client(),
         {:ok, %{status: 200} = env} <- ApiClient.search_jobs(client, sid) do
      state = state_from_response(env)
      tick = calculate_next_tick(state)

      unless state.is_done do
        Logger.info(
          "Job #{sid} is #{Float.ceil(state.done_progress * 100, 2)}% complete, " <>
            "rescheduling update in #{tick} seconds"
        )
      end

      Process.send_after(self(), :tick, tick * 1000)
      {:noreply, state}
    else
      {:ok, %{status: 404}} ->
        Logger.error("Job #{sid} does not exist - Shutting down")
        {:stop, :normal, state}

      reason ->
        Logger.error("Job #{sid} failed with reason #{inspect(reason)} - Shutting down")
        {:stop, :normal, state}
    end
  end

  def handle_info(:tick, %State{sid: _sid, is_done: true} = state) do
    IO.puts("Job is ready for processing!")
    {:noreply, state}
  end

  @spec state_from_response(Tesla.Env.t()) :: State.t()
  defp state_from_response(%{
         status: 200,
         body: %{"entry" => [%{"content" => content} = entry | _rest]}
       }) do
    State.new(entry)
    |> Map.merge(State.new(content), fn
      _key, old_value, new_value when is_nil(new_value) -> old_value
      _key, _old_value, new_value -> new_value
    end)
  end

  # Calculates the next tick interval for fetching the job meta data.
  #
  # Algorithm is as follows:
  #  T = time difference since job was started
  #  P = job progress (normalized value)
  #
  #  seconds until next check = (T * (1 / P)) - T
  #
  @spec calculate_next_tick(State.t()) :: Integer.t()
  defp calculate_next_tick(%State{done_progress: 1}), do: 0

  defp calculate_next_tick(%State{published: published, done_progress: progress}) do
    with {:ok, published_dt, _offset} = DateTime.from_iso8601(published),
         diff <- DateTime.diff(DateTime.utc_now(), published_dt, :second) do
      D.mult(diff, D.div(1, D.from_float(progress)))
      |> D.sub(diff)
      |> D.to_float()
      |> ceil()
    end
  end
end
