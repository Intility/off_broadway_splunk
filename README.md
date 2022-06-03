# OffBroadway.Splunk

![pipeline status](https://github.com/Intility/off_broadway_splunk/actions/workflows/elixir.yaml/badge.svg?event=push)

A Splunk event consumer for [Broadway](https://github.com/dashbitco/broadway).

Broadway producer acts as a consumer for the given Splunk `SID` (Search ID).
Splunk does not really provide a queue we can consume events from, so we need to fetch events using the
Splunk Web API. When the Broadway pipeline starts, it will also start a `OffBroadway.Splunk.Leader` process
which will poll Splunk for job status of the given `SID`. Once Splunk reports that the job is ready to be consumed
the `OffBroadway.Splunk.Producer` will start fetching events and passing them through the Broadway system.

Read the full documentation [here](https://hexdocs.pm/off_broadway_splunk/readme.html).

## Installation

This package is [available in Hex](https://hex.pm/packages/off_broadway_splunk), and can be installed
by adding `off_broadway_splunk` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:off_broadway_splunk, "~> 1.1"}
  ]
end
```

## Usage

The `OffBroadway.Splunk.SplunkClient` tries to read the following configuration from `config.exs`.

```elixir
# config.exs

config :off_broadway_splunk, :splunk_client,
  base_url: System.get_env("SPLUNK_BASE_URL", "https://splunk.example.com"),
  api_token: System.get_env("SPLUNK_API_TOKEN", "your-api-token-here")
```

Options for the `OffBroadway.Splunk.SplunkClient` can be configured either in `config.exs` or passed as
options directly to the `OffBroadway.Splunk.Producer` module. Options are merged, with the passed options
taking precedence over those configured in `config.exs`.

```elixir
# my_broadway.ex

defmodule MyBroadway do
  use Broadway

  alias Broadway.Message

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module:
          {OffBroadway.Splunk.Producer,
           sid: "SID-to-consume",
           config: [api_token: "override-api-token"]}
      ],
      processors: [
        default: []
      ],
      batchers: [
        default: [
          batch_size: 500,
          batch_timeout: 5000
        ]
      ]
    )
  end

  ...callbacks...
end
```

### Processing messages

In order to process incoming messages, we need to implement some callback functions.

```elixir
defmodule MyBroadway do
  use Broadway

  alias Broadway.Message

  ...start_link...

  @impl true
  def handle_message(_, %Message{data: data} ,_) do
    message
    |> Message.update_data(fn -> ...whatever... end)
  end

  @impl true
  def handle_batch(_batcher, messages, _batch_info, _context) do
    IO.puts("Received a batch of #{length(messages)} messages!")
    messages
  end
end
```

For the sake of the example, we're not really doing anything here. Whenever we're receiving a batch of messages, we just prints out a
message saying "Received a batch of messages!", and for each message we run `Message.update_data/2` passing a function that can process
that message ie. by doing some calculations on the data or something else.
