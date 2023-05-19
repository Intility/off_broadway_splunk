defmodule OffBroadway.Splunk.JobFetcher do
  @moduledoc """
  GenServer responsible for polling Splunk for available report or alert
  jobs. Whenever jobs are available it will notify the `OffBroadway.Splunk.Producer`
  so it can decide if it should produce messages from the jobs.
  """

  defmodule Job do
    @moduledoc false

    defstruct [
      :name,
      :published,
      :is_done,
      :is_zombie,
      :is_scheduled,
      :start,
      :ttl
    ]

    @type t :: %__MODULE__{
            name: String.t(),
            published: String.t(),
            is_done: boolean(),
            is_zombie: boolean(),
            is_scheduled: boolean(),
            start: integer(),
            ttl: integer()
          }

    use ExConstructor

    def new(map_or_kwlist, opts \\ []) do
      case super(map_or_kwlist, opts) do
        %{published: published} = struct when is_binary(published) ->
          {:ok, datetime, _offset} = DateTime.from_iso8601(published)
          %{struct | published: datetime}

        struct ->
          struct
      end
    end
  end

  use GenServer
  require Logger

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    client = opts[:splunk_client]
    {:ok, client_opts} = client.init(opts)

    state = %{
      broadway: opts[:broadway],
      jobs: [],
      name: opts[:name],
      refetch_interval: opts[:refetch_interval],
      refetch_timer: nil,
      splunk_client: {client, client_opts}
    }

    {:ok, state, {:continue, :receive_jobs}}
  end

  @impl true
  def handle_continue(:receive_jobs, state),
    do: {:noreply, %{state | refetch_timer: schedule_receive_jobs(0)}}

  @impl true
  def handle_info(:receive_jobs, %{refetch_timer: timer, refetch_interval: interval} = state) do
    timer && Process.cancel_timer(timer)

    new_state =
      receive_jobs(state)
      |> then(fn {:ok, response} -> update_state_from_response(response, state) end)

    Broadway.producer_names(state.broadway)
    |> Enum.random()
    |> GenStage.cast({:receive_jobs, jobs: new_state.jobs})

    {:noreply, %{new_state | refetch_timer: schedule_receive_jobs(interval)}}
  end

  @spec receive_jobs(state :: map()) :: map()
  defp receive_jobs(%{name: name, splunk_client: {client, client_opts}}) do
    metadata = %{name: name, jobs_count: 0}

    :telemetry.span(
      [:off_broadway_splunk, :receive_jobs],
      metadata,
      fn ->
        {:ok, %{status: 200, body: %{"entry" => jobs}}} =
          env = client.receive_status(name, client_opts)

        {env, %{metadata | jobs_count: length(jobs)}}
      end
    )
  end

  @spec update_state_from_response(response :: Tesla.Env.t(), state :: map()) :: map()
  defp update_state_from_response(%{body: %{"entry" => jobs}}, state) do
    jobs =
      Enum.reduce(jobs, state.jobs, fn job, acc ->
        [merge_non_nil_fields(Job.new(job), Job.new(Map.get(job, "content"))) | acc]
      end)

    %{state | jobs: jobs}
  end

  @spec merge_non_nil_fields(map_a :: map(), map_b :: map()) :: map()
  defp merge_non_nil_fields(map_a, map_b) do
    Map.merge(map_a, map_b, fn
      _key, old_value, new_value when is_nil(new_value) -> old_value
      _key, _old_value, new_value -> new_value
    end)
  end

  defp schedule_receive_jobs(interval),
    do: Process.send_after(self(), :receive_jobs, interval)
end
