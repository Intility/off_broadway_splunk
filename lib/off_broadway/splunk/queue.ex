defmodule OffBroadway.Splunk.Queue do
  @moduledoc """
  `GenServer` responsible for fetching jobs from the Splunk Web API, and maintain a
  queue for which jobs that should be processed by the `OffBroadway.Splunk.Producer`.
  This process is automatically started as part of the `Broadway` supervision tree.
  """

  defmodule Job do
    @moduledoc false

    defstruct [
      :name,
      :published,
      :is_done,
      :is_zombie,
      :is_scheduled
    ]

    @type t :: %__MODULE__{
            name: String.t(),
            published: String.t(),
            is_done: boolean(),
            is_zombie: boolean(),
            is_scheduled: boolean()
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

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    client = opts[:splunk_client]
    {:ok, client_opts} = client.init(opts)

    state = %{
      name: opts[:name],
      broadway: opts[:broadway],
      current_job: nil,
      queue: :queue.new(),
      completed_jobs: MapSet.new(),
      refetch_timer: nil,
      refetch_interval: opts[:refetch_interval],
      splunk_client: {client, client_opts}
    }

    {:ok, state, {:continue, :receive_jobs}}
  end

  @impl true
  def handle_continue(:receive_jobs, state),
    do: {:noreply, %{state | refetch_timer: schedule_receive_jobs(0)}}

  def handle_continue(:maybe_notify_producers, %{current_job: nil} = state) do
    case :queue.out(state.queue) do
      {{:value, job}, queue} ->
        Broadway.producer_names(state.broadway)
        |> Enum.random()
        |> GenStage.call({:ready, sid: job.name})

        {:noreply, %{state | current_job: job, queue: queue}}

      {:empty, _queue} ->
        {:noreply, state}
    end
  end

  def handle_continue(:maybe_notify_producers, state), do: {:noreply, state}

  @impl true
  def handle_call(:enqueue_job, _from, %{current_job: current, completed_jobs: completed} = state) do
    case :queue.out(state.queue) do
      {{:value, job}, queue} ->
        {:reply, {:ok, job.name},
         %{state | current_job: job, queue: queue, completed_jobs: MapSet.put(completed, current)}}

      {:empty, queue} ->
        {:reply, {:ok, nil},
         %{state | current_job: nil, queue: queue, completed_jobs: MapSet.put(completed, current)}}
    end
  end

  @impl true
  def handle_info(:receive_jobs, %{refetch_timer: timer, refetch_interval: interval} = state) do
    timer && Process.cancel_timer(timer)

    queue =
      receive_jobs(state)
      |> then(fn {:ok, response} -> update_queue_from_response(response, state) end)

    {:noreply, %{state | queue: queue, refetch_timer: schedule_receive_jobs(interval)},
     {:continue, :maybe_notify_producers}}
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

  @spec update_queue_from_response(response :: Tesla.Env.t(), state :: map()) :: :queue.queue()
  defp update_queue_from_response(%{body: %{"entry" => jobs}}, state) do
    jobs =
      Enum.map(jobs, &merge_non_nil_fields(Job.new(&1), Job.new(Map.get(&1, "content"))))
      |> Enum.reject(& &1.is_zombie)
      |> Enum.filter(& &1.is_done)
      |> Enum.sort_by(& &1.published, {:asc, DateTime})

    :queue.fold(
      fn job, acc ->
        with false <- job == state.current_job,
             false <- :queue.member(job, acc),
             false <- MapSet.member?(state.completed_jobs, job) do
          :queue.in(job, acc)
        else
          true -> acc
        end
      end,
      state.queue,
      :queue.from_list(jobs)
    )
  end

  @spec merge_non_nil_fields(map_a :: map(), map_b :: map()) :: map()
  defp merge_non_nil_fields(map_a, map_b) do
    Map.merge(map_a, map_b, fn
      _key, old_value, new_value when is_nil(new_value) -> old_value
      _key, _old_value, new_value -> new_value
    end)
  end

  @spec schedule_receive_jobs(interval :: non_neg_integer()) :: reference()
  defp schedule_receive_jobs(interval),
    do: Process.send_after(self(), :receive_jobs, interval)
end
