defmodule OffBroadway.Splunk.Options do
  @moduledoc false

  def definition do
    [
      name: [
        required: true,
        type: {
          :custom,
          __MODULE__,
          :type_non_empty_string,
          [[{:name, :name}]]
        },
        doc: """
        The report or alert name for the Splunk job we want to consume events from.
        """
      ],
      receive_interval: [
        type: :non_neg_integer,
        doc: """
        The duration (in milliseconds) for which the producer waits before
        making a request for more messages.
        """,
        default: 5000
      ],
      refetch_interval: [
        type: :non_neg_integer,
        doc: """
        The duration (in milliseconds) to wait before fetching new jobs to be processed.
        """,
        default: 60_000
      ],
      only_new: [
        doc: """
        If set to `true`, the pipeline will skip adding any existing jobs to the initial queue.
        """,
        type: :boolean,
        default: false
      ],
      only_latest: [
        doc: """
        If set to `true`, the pipeline will only add the most recent job to the initial queue.
        """,
        type: :boolean,
        default: false
      ],
      shutdown_timeout: [
        type: :timeout,
        doc: """
        The duration (in milliseconds) Broadway should wait before timing out when
        trying to stop the pipeline.
        """,
        default: :infinity
      ],
      on_success: [
        type: :atom,
        doc: """
        Configures the acking behaviour for successful messages. See the "Acknowledgements"
        section below for all the possible values.
        """,
        default: :ack
      ],
      on_failure: [
        type: :atom,
        doc: """
        Configures the acking behaviour for failed messages. See the "Acknowledgements"
        section below for all the possible values.
        """,
        default: :noop
      ],
      splunk_client: [
        doc: """
        A module that implements the `OffBroadway.Splunk.Client` behaviour.
        This module is responsible for fetching and acknowledging the messages
        from Splunk. All options passed to the producer will also be forwarded to
        the client.
        """,
        default: OffBroadway.Splunk.SplunkClient
      ],
      config: [
        type: :keyword_list,
        keys: [
          base_url: [type: :string, doc: "Base URL to Splunk instance."],
          api_token: [
            doc: "API token used to authenticate on the Splunk instance.",
            type: :string
          ],
          api_version: [
            doc: """
            Some API endpoints are [available](https://docs.splunk.com/Documentation/Splunk/9.0.3/RESTREF/RESTsearch)
            in multiple versions. Sets the API version to use (where applicable).
            """,
            type: {:in, ["v1", "v2"]},
            default: "v2"
          ],
          max_events: [
            doc: """
            If set to a positive integer, automatically shut down the pipeline after consuming
            `max_events` messages from the Splunk API.
            """,
            type: {:custom, __MODULE__, :type_nil_or_pos_integer, [[{:name, :max_events}]]}
          ]
        ],
        doc: """
        A set of config options that overrides the default config for the `splunk_client`
        module. Any option set here can also be configured in `config.exs`.
        """,
        default: []
      ],
      test_pid: [type: :pid, doc: false],
      message_server: [type: :pid, doc: false]
    ]
  end

  def type_non_empty_string("", [{:name, name}]),
    do: {:error, "expected :#{name} to be a non-empty string, got: \"\""}

  def type_non_empty_string(value, _) when not is_nil(value) and is_binary(value),
    do: {:ok, value}

  def type_non_empty_string(value, [{:name, name}]),
    do: {:error, "expected :#{name} to be a non-empty string, got: #{inspect(value)}"}

  def type_nil_or_pos_integer(nil, [{:name, _}]), do: {:ok, nil}

  def type_nil_or_pos_integer(value, [{:name, _}]) when is_integer(value) and value > 0,
    do: {:ok, value}

  def type_nil_or_pos_integer(value, [{:name, name}]),
    do: {:error, "expected :#{name} to be nil or a positive integer, got: #{inspect(value)}"}
end
