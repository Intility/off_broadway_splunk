defmodule OffBroadway.Splunk.SplunkClientTest do
  use ExUnit.Case

  alias OffBroadway.Splunk.SplunkClient
  alias Broadway.Message

  import ExUnit.CaptureLog
  import Tesla.Mock

  @sid1 "8CB53D79-587A-43EE-95CC-14256C65EF95"
  @sid2 "non-existing"

  @message1 %{
    "_bkt" => "my-index~8CB53D79-587A-43EE-95CC-14256C65EF95",
    "_cd" => "329:7062435",
    "_eventtype_color" => "none",
    "_indextime" => "1656323118",
    "_raw" => " {\"field1\": \"some data\", \"field2\": \"more data\"} ",
    "_serial" => "344",
    "_si" => [
      "splunk.example.com",
      "my-index"
    ],
    "_sourcetype" => "my-index",
    "_time" => "2022-06-27T03:20:02.000+02:00",
    "field1" => "some data",
    "field2" => "more data"
  }
  @message2 %{
    "_bkt" => "my-source~8CB53D79-587A-43EE-95CC-14256C65EF95",
    "_cd" => "329:7062366",
    "_eventtype_color" => "none",
    "_indextime" => "1656323118",
    "_raw" => " {\"field1\": \"some other data\", \"field2\": \"more other data\"} ",
    "_serial" => "344",
    "_si" => [
      "splunk.example.com",
      "my-index"
    ],
    "_sourcetype" => "my-index",
    "_time" => "2022-06-27T03:20:02.000+02:00",
    "field1" => "some other data",
    "field2" => "more other data"
  }

  setup do
    mock(fn
      %{method: :get, url: "https://splunk.example.com/services/search/v2/jobs/#{@sid1}/events"} ->
        %Tesla.Env{status: 200, body: %{"results" => [@message1, @message2]}}

      %{method: :get, url: "https://splunk.example.com/services/search/v2/jobs/#{@sid2}/events"} ->
        %Tesla.Env{
          status: 404,
          body: %{"messages" => [%{"type" => "FATAL", "text" => "Unknown sid."}]}
        }
    end)
  end

  describe "receive_messages/3" do
    setup do
      {:ok,
       %{
         base_opts: [
           sid: @sid1,
           config: [
             endpoint: :events,
             base_url: "https://splunk.example.com",
             api_token: "secret-api-token",
             api_version: "v2"
           ]
         ]
       }}
    end

    test "init/1 returns normalized client options", %{base_opts: base_opts} do
      assert {:ok,
              [
                endpoint: :events,
                base_url: "https://splunk.example.com",
                api_token: "secret-api-token",
                api_version: "v2"
              ]} = SplunkClient.init(base_opts)
    end

    test "returns a list of Broadway.Message with :data and :acknowledger set", %{
      base_opts: base_opts
    } do
      {:ok, opts} = SplunkClient.init(base_opts)
      [message1, message2] = SplunkClient.receive_messages(@sid1, 10, opts)

      assert message1.data == @message1
      assert message2.data == @message2

      assert message1.acknowledger ==
               {SplunkClient, @sid1, %{receipt: %{id: "splunk.example.com;my-index;329:7062435"}}}
    end

    test "if the request fails, returns an empty list and log the error", %{base_opts: base_opts} do
      {:ok, opts} = SplunkClient.init(base_opts)

      assert capture_log(fn ->
               assert [] == SplunkClient.receive_messages(@sid2, 10, opts)
             end) =~ """
             [error] Unable to fetch events from Splunk SID #{@sid2}. \
             Request failed with status code: 404 and reason: [%{\"text\" => \"Unknown sid.\", \"type\" => \"FATAL\"}].
             """
    end
  end

  describe "ack/3" do
    setup do
      {:ok,
       %{
         base_opts: [
           sid: @sid1,
           config: [
             endpoint: :events,
             base_url: "https://splunk.example.com",
             api_token: "secret-api-token"
           ],
           on_success: :ack,
           on_failure: :noop
         ]
       }}
    end

    test "emits a telemetry event when acking messages", %{base_opts: base_opts} do
      self = self()
      {:ok, opts} = SplunkClient.init(base_opts)

      ack_data = %{receipt: %{id: "1"}}
      fill_persistent_term(opts[:sid], base_opts)

      capture_log(fn ->
        :ok =
          :telemetry.attach(
            "ack_test",
            [:off_broadway_splunk, :receive_messages, :ack],
            fn name, measurements, metadata, _ ->
              send(self, {:telemetry_event, name, measurements, metadata})
            end,
            nil
          )
      end)

      SplunkClient.ack(
        opts[:sid],
        [%Message{acknowledger: {SplunkClient, opts[:sid], ack_data}, data: nil}],
        []
      )

      assert_receive {:telemetry_event, [:off_broadway_splunk, :receive_messages, :ack],
                      %{time: _}, %{sid: _, receipt: _}}
    end
  end

  defp fill_persistent_term(ack_ref, base_opts) do
    :persistent_term.put(ack_ref, %{
      sid: base_opts[:sid],
      config: base_opts[:config],
      on_success: base_opts[:on_success] || :ack,
      on_failure: base_opts[:on_failure] || :noop
    })
  end
end
