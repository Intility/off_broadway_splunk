import Config

case config_env() do
  :dev ->
    config :mix_test_watch, tasks: ["test --cover"]

    config :tesla, adapter: {Tesla.Adapter.Hackney, [recv_timeout: 30_000]}

    config :off_broadway_splunk, :splunk_client,
      base_url: System.get_env("SPLUNK_BASE_URL", "https://splunk.example.com"),
      api_token: System.get_env("SPLUNK_API_TOKEN", "your-api-token-here")

  :test ->
    config :tesla, adapter: Tesla.Mock
end
