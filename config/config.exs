import Config

config :tesla, adapter: {Tesla.Adapter.Hackney, [recv_timeout: 30_000]}

config :off_broadway_splunk, :api_client,
  base_url: System.get_env("SPLUNK_BASE_URL", "https://splunk.example.com"),
  api_token: System.get_env("SPLUNK_API_TOKEN", "your-api-token-here")

import_config "#{config_env()}.exs"
