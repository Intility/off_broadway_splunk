defmodule OffBroadway.Splunk.ProducerTest do
  @moduledoc """
  This test case is basically stolen from the `BroadwaySQS` project
  and adapted to the Splunk producer case.
  """
  use ExUnit.Case, async: true

  alias Broadway.Message
  import ExUnit.CaptureLog

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
    def init(opts), do: {:ok, opts}

    @impl true
    def receive_status(_sid, _opts) do
      {:ok,
       %{
         status: 200,
         body: %{
           "entry" => [
             %{
               "published" => "2022-06-28T15:00:02.000+02:00",
               "content" => %{
                 "doneProgress" => 1,
                 "eventCount" => 200,
                 "isDone" => true,
                 "isZombie" => false,
                 "sid" => "8CB53D79-587A-43EE-95CC-14256C65EF95"
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
    test "when the sid is not present" do
      message = """
      invalid configuration given to OffBroadway.Splunk.Producer.prepare_for_start/2, \
      required option :sid not found, received options: []\
      """

      assert_raise(ArgumentError, message, fn ->
        prepare_for_start_module_opts([])
      end)
    end

    test "when the sid is nil" do
      assert_raise(
        ArgumentError,
        ~r/expected :sid to be a non-empty string, got: nil/,
        fn -> prepare_for_start_module_opts(sid: nil) end
      )
    end

    test "when the sid is an empty string" do
      assert_raise(
        ArgumentError,
        ~r/expected :sid to be a non-empty string, got: \"\"/,
        fn -> prepare_for_start_module_opts(sid: "") end
      )
    end

    test "when the sid is an atom" do
      assert_raise(
        ArgumentError,
        ~r/expected :sid to be a non-empty string, got: :sid_atom/,
        fn -> prepare_for_start_module_opts(sid: :sid_atom) end
      )
    end

    test "when the sid is a string" do
      assert {[_child_spec],
              [
                producer: [
                  module: {OffBroadway.Splunk.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} = prepare_for_start_module_opts(sid: "8CB53D79-587A-43EE-95CC-14256C65EF95")

      assert result_module_opts[:sid] == "8CB53D79-587A-43EE-95CC-14256C65EF95"
    end

    test ":config is optional with default value []" do
      assert {[_child_spec],
              [
                producer: [
                  module: {OffBroadway.Splunk.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} = prepare_for_start_module_opts(sid: "8CB53D79-587A-43EE-95CC-14256C65EF95")

      assert result_module_opts[:config] == [endpoint: :events]
    end

    test ":config should be a keyword list" do
      assert {[_child_spec],
              [
                producer: [
                  module: {OffBroadway.Splunk.Producer, result_module_opts},
                  concurrency: 1
                ]
              ]} =
               prepare_for_start_module_opts(
                 sid: "8CB53D79-587A-43EE-95CC-14256C65EF95",
                 config: [base_url: "https://api.splunk.example.com", api_token: "super-secret"]
               )

      assert result_module_opts[:config] == [
               endpoint: :events,
               base_url: "https://api.splunk.example.com",
               api_token: "super-secret"
             ]

      assert_raise(
        ArgumentError,
        ~r/expected :config to be a keyword list, got: :an_atom/,
        fn ->
          prepare_for_start_module_opts(
            sid: "8CB53D79-587A-43EE-95CC-14256C65EF95",
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

    test "stop trying to receive new messages after start draining" do
      {:ok, message_server} = MessageServer.start_link()
      broadway_name = new_unique_name()
      {:ok, pid} = start_broadway(broadway_name, message_server, receive_interval: 5_000)

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

      MessageServer.push_messages(message_server, [2])

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

  defp start_broadway(broadway_name \\ new_unique_name(), message_server, opts \\ []) do
    Broadway.start_link(
      Forwarder,
      build_broadway_opts(broadway_name, opts,
        splunk_client: FakeSplunkClient,
        sid: "8CB53D79-587A-43EE-95CC-14256C65EF95",
        config: [
          base_url: "https://api.splunk.example.com",
          api_token: "secret-token"
        ],
        receive_interval: 0,
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
