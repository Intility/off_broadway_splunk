defmodule OffBroadway.Splunk.ProducerTest do
  @moduledoc """
  This test case is basically stolen from the `BroadwaySQS` project
  and adapted to the Splunk producer case.
  """
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  alias Broadway.Message

  defmodule MessageServer do
    def start_link, do: Agent.start_link(fn -> [] end)

    def push_messages(server, messages),
      do: Agent.update(server, fn queue -> queue ++ messages end)

    def take_messages(server, amount),
      do: Agent.get_and_update(server, &Enum.split(&1, amount))
  end

  defmodule FakeSplunkClient do
    @behaviour OffBroadway.Splunk.Client
    @behaviour Broadway.Acknowledger

    @impl true
    def init(opts) do
      client_opts =
        Keyword.take(opts, [:message_server, :test_pid])
        |> Keyword.merge(opts[:config])

      {:ok, client_opts}
    end

    @impl true
    def receive_status(_sid, _opts) do
      {:ok,
       %{
         status: 200,
         body: %{
           "entry" => [
             %{
               "name" => "8CB53D79-587A-43EE-95CC-14256C65EF95",
               "published" => "2022-06-28T15:00:02.000+02:00",
               "content" => %{
                 "isDone" => true,
                 "isScheduled" => true,
                 "isZombie" => false
               }
             }
           ]
         }
       }}
    end

    @impl true
    def receive_messages(_sid, demand, opts) do
      messages = MessageServer.take_messages(opts[:message_server], demand)
      send(opts[:test_pid], {:messages_received, length(messages)})

      for msg <- messages do
        ack_data = %{
          receipt: %{id: "splunk.example.com;my-index;329:7062435"},
          test_pid: opts[:test_pid]
        }

        metadata = %{custom: "custom-data"}
        %Message{data: msg, metadata: metadata, acknowledger: {__MODULE__, :ack_ref, ack_data}}
      end
    end

    @impl true
    def ack(_ack_ref, successful, _failed) do
      [%Message{acknowledger: {_, _, %{test_pid: test_pid}}} | _] = successful
      send(test_pid, {:messages_acknowledged, length(successful)})
    end
  end

  defmodule ErrorSplunkClient do
    @behaviour OffBroadway.Splunk.Client

    @impl true
    def init(opts) do
      client_opts =
        Keyword.take(opts, [:message_server, :test_pid])
        |> Keyword.merge(opts[:config])

      {:ok, client_opts}
    end

    @impl true
    def receive_status(_sid, _opts), do: {:error, :timeout}

    @impl true
    def receive_messages(_sid, _demand, _opts), do: {:error, :timeout}
  end

  defmodule Forwarder do
    use Broadway

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
    def init(opts), do: {:ok, opts}

    def handle_message(_, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message.data, message.metadata})
      message
    end

    def handle_batch(_, messages, _, _) do
      messages
    end
  end

  defp prepare_for_start_module_opts(module_opts) do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)

    try do
      OffBroadway.Splunk.Producer.prepare_for_start(Forwarder,
        producer: [
          module: {OffBroadway.Splunk.Producer, module_opts},
          concurrency: 1
        ]
      )
    after
      stop_broadway(pid)
    end
  end

  describe "prepare_for_start/2 validation" do
    test "when :name is not present" do
      assert_raise(
        ArgumentError,
        ~r/required :name option not found, received options: \[\]/,
        fn ->
          prepare_for_start_module_opts([])
        end
      )
    end

    test "when :name is nil" do
      assert_raise(
        ArgumentError,
        ~r/expected :name to be a non-empty string, got: nil/,
        fn -> prepare_for_start_module_opts(name: nil) end
      )
    end

    test "when :name is an empty string" do
      assert_raise(
        ArgumentError,
        ~r/expected :name to be a non-empty string, got: \"\"/,
        fn -> prepare_for_start_module_opts(name: "") end
      )
    end

    test "when :name is an atom" do
      assert_raise(
        ArgumentError,
        ~r/expected :name to be a non-empty string, got: :name_atom/,
        fn -> prepare_for_start_module_opts(name: :name_atom) end
      )
    end

    test "when :name is a string" do
      assert {[],
              [
                producer: [
                  module: {OffBroadway.Splunk.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} = prepare_for_start_module_opts(name: "My fine report")

      assert result_module_opts[:name] == "My fine report"
    end

    test "when :max_events is a negative integer" do
      assert_raise(
        ArgumentError,
        ~r/expected :max_events to be nil or a positive integer, got: -10/,
        fn ->
          prepare_for_start_module_opts(
            name: "My fine report",
            config: [max_events: -10]
          )
        end
      )
    end

    test "when :max_events is nil" do
      assert {[],
              [
                producer: [
                  module: {OffBroadway.Splunk.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 name: "My fine report",
                 config: [
                   max_events: nil,
                   base_url: "https://api.splunk.example.com",
                   api_token: "super-secret"
                 ]
               )

      assert result_module_opts[:config][:max_events] == nil
    end

    test "when :max_events is a positive integer" do
      assert {[],
              [
                producer: [
                  module: {OffBroadway.Splunk.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 name: "My fine report",
                 config: [
                   max_events: 10,
                   base_url: "https://api.splunk.example.com",
                   api_token: "super-secret"
                 ]
               )

      assert result_module_opts[:config][:max_events] == 10
    end

    test ":only_latest is default false" do
      assert {[],
              [
                producer: [
                  module: {OffBroadway.Splunk.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} = prepare_for_start_module_opts(name: "My fine report")

      assert result_module_opts[:only_latest] == false
    end

    test ":config is optional with default values" do
      assert {[],
              [
                producer: [
                  module: {OffBroadway.Splunk.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} = prepare_for_start_module_opts(name: "My fine report")

      assert result_module_opts[:config] == [api_version: "v2"]
    end

    test ":config when :api_version is invalid" do
      assert_raise(
        ArgumentError,
        ~r/invalid value for :api_version option: expected one of \["v1", "v2"\], got: "v3"/,
        fn ->
          prepare_for_start_module_opts(
            name: "My fine report",
            config: [api_version: "v3"]
          )
        end
      )
    end

    test ":config should be a keyword list" do
      assert {[],
              [
                producer: [
                  module: {OffBroadway.Splunk.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 name: "My fine report",
                 config: [
                   base_url: "https://api.splunk.example.com",
                   api_token: "super-secret",
                   api_version: "v1"
                 ]
               )

      assert result_module_opts[:config] == [
               base_url: "https://api.splunk.example.com",
               api_token: "super-secret",
               api_version: "v1"
             ]

      assert_raise(
        ArgumentError,
        ~r/invalid value for :config option: expected keyword list, got: :an_atom/,
        fn ->
          prepare_for_start_module_opts(
            name: "My fine report",
            config: :an_atom
          )
        end
      )
    end
  end

  describe "producer" do
    test "receive messages when the queue has less than the demand" do
      {:ok, message_server} = MessageServer.start_link()
      {:ok, pid} = start_broadway(message_server)

      MessageServer.push_messages(message_server, 1..5)

      assert_receive {:messages_received, 5}

      for msg <- 1..5 do
        assert_receive {:message_handled, ^msg, _}
      end

      stop_broadway(pid)
    end

    test "receive messages with the metadata defined by the Splunk client" do
      {:ok, message_server} = MessageServer.start_link()
      {:ok, pid} = start_broadway(message_server)
      MessageServer.push_messages(message_server, 1..5)

      assert_receive {:message_handled, _, %{custom: "custom-data"}}

      stop_broadway(pid)
    end

    test "keep receiving messages when the queue has more than the demand" do
      {:ok, message_server} = MessageServer.start_link()
      MessageServer.push_messages(message_server, 1..20)
      {:ok, pid} = start_broadway(message_server)

      assert_receive {:messages_received, 10}

      for msg <- 1..10 do
        assert_receive {:message_handled, ^msg, _}
      end

      assert_receive {:messages_received, 5}

      for msg <- 11..15 do
        assert_receive {:message_handled, ^msg, _}
      end

      assert_receive {:messages_received, 5}

      for msg <- 16..20 do
        assert_receive {:message_handled, ^msg, _}
      end

      assert_receive {:messages_received, 0}

      stop_broadway(pid)
    end

    test "keep trying to receive new messages when the queue is empty" do
      {:ok, message_server} = MessageServer.start_link()
      {:ok, pid} = start_broadway(message_server)

      MessageServer.push_messages(message_server, [13])
      assert_receive {:messages_received, 1}
      assert_receive {:message_handled, 13, _}

      assert_receive {:messages_received, 0}
      refute_receive {:message_handled, _, _}

      MessageServer.push_messages(message_server, [14, 15])
      assert_receive {:messages_received, 2}
      assert_receive {:message_handled, 14, _}
      assert_receive {:message_handled, 15, _}

      stop_broadway(pid)
    end

    test "keep trying to receive new messages after endpoint error" do
      broadway_name = new_unique_name()
      {:ok, message_server} = MessageServer.start_link()

      {:ok, pid} =
        start_broadway(message_server, broadway_name, splunk_client: ErrorSplunkClient)

      MessageServer.push_messages(message_server, [10])
      refute_receive {:messages_received, _}, 100

      stop_broadway(pid)
    end

    test "stop trying to receive new messages after start draining" do
      broadway_name = new_unique_name()
      {:ok, message_server} = MessageServer.start_link()
      {:ok, pid} = start_broadway(message_server, broadway_name, receive_interval: 5_000)

      [producer] = Broadway.producer_names(broadway_name)
      assert_receive {:messages_received, 0}

      # Drain and explicitly ask it to receive messages but it shouldn't work
      Broadway.Topology.ProducerStage.drain(producer)
      send(producer, :receive_messages)

      refute_receive {:messages_received, _}, 10
      stop_broadway(pid)
    end

    test "acknowledged messages" do
      {:ok, message_server} = MessageServer.start_link()
      {:ok, pid} = start_broadway(message_server)

      MessageServer.push_messages(message_server, 1..20)
      assert_receive {:messages_acknowledged, 10}
      assert_receive {:messages_acknowledged, 10}

      stop_broadway(pid)
    end

    test "emit a telemetry start event with demand" do
      self = self()
      {:ok, message_server} = MessageServer.start_link()
      {:ok, pid} = start_broadway(message_server)

      capture_log(fn ->
        :ok =
          :telemetry.attach(
            "start_test",
            [:off_broadway_splunk, :receive_messages, :start],
            fn name, measurements, metadata, _ ->
              send(self, {:telemetry_event, name, measurements, metadata})
            end,
            nil
          )
      end)

      assert_receive {:telemetry_event, [:off_broadway_splunk, :receive_messages, :start],
                      %{system_time: _}, %{sid: _, demand: 10}}

      stop_broadway(pid)
    end

    test "emit a telemetry stop event with received count" do
      self = self()
      {:ok, message_server} = MessageServer.start_link()
      {:ok, pid} = start_broadway(message_server)

      capture_log(fn ->
        :ok =
          :telemetry.attach(
            "stop_test",
            [:off_broadway_splunk, :receive_messages, :stop],
            fn name, measurements, metadata, _ ->
              send(self, {:telemetry_event, name, measurements, metadata})
            end,
            nil
          )
      end)

      assert_receive {:telemetry_event, [:off_broadway_splunk, :receive_messages, :stop],
                      %{duration: _}, %{sid: _, received: _, demand: 10}}

      stop_broadway(pid)
    end
  end

  defp start_broadway(message_server, broadway_name \\ new_unique_name(), opts \\ []) do
    {client, opts} = Keyword.pop(opts, :splunk_client)

    Broadway.start_link(
      Forwarder,
      build_broadway_opts(broadway_name, opts,
        splunk_client: client || FakeSplunkClient,
        name: "My fine report",
        config: [
          base_url: "https://api.splunk.example.com",
          api_token: "secret-token"
        ],
        receive_interval: 0,
        refetch_interval: 0,
        test_pid: self(),
        message_server: message_server
      )
    )
  end

  defp build_broadway_opts(broadway_name, opts, producer_opts) do
    [
      name: broadway_name,
      context: %{test_pid: self()},
      producer: [
        module: {OffBroadway.Splunk.Producer, Keyword.merge(producer_opts, opts)},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 1]
      ],
      batchers: [
        default: [
          batch_size: 10,
          batch_timeout: 50,
          concurrency: 1
        ]
      ]
    ]
  end

  defp new_unique_name() do
    :"Broadway#{System.unique_integer([:positive, :monotonic])}"
  end

  defp stop_broadway(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end
end
