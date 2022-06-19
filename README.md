# OffBroadway.Splunk

[![pipeline status](https://gitlab.intility.com/soc/off_broadway_splunk/badges/master/pipeline.svg)](https://gitlab.intility.com/soc/off_broadway_splunk/-/commits/master)
[![coverage report](https://gitlab.intility.com/soc/off_broadway_splunk/badges/master/coverage.svg)](https://gitlab.intility.com/soc/off_broadway_splunk/-/commits/master)

A Splunk event consumer for [Broadway](https://github.com/dashbitco/broadway).

Read the full documentation [here](http://soc.pages.intility.com/off_broadway_splunk)!

## How does it work

Generally speaking, Broadway consumes events from systems such as RabbitMQ, Amazon SQS, Apache Kafka, and so on.
Splunk does not support any concept of acknowledgement (as far as I'm aware of), so this library consume events using
the Splunk Web API. A producer is assigned a Splunk `SID` (Search ID) to consume, then it will poll Splunk for status on the
prepare job (which will make events for that `SID` available to download using the Web API) until the job is ready. Next events
will be downloaded in parallel (depending on how many events the `SID` contains) and handed over to a consumer.

## Installation

This package is not yet available in [Hex](https://hex.pm/docs/publish), so it must be installed
directly from the [Intility Gitlab](https://gitlab.intility.com) server by adding `off_broadway_splunk` to your list of dependencies in `mix.exs`.

```elixir
def deps do
  [
    {:off_broadway_splunk, git: "git@gitlab.intility.com:soc/off_broadway_splunk.git", tag: "1.0.0"}
  ]
end
```

## Usage

```elixir
defmodule MyBroadway do
  use Broadway

  alias Broadway.Message

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {OffBroadway.Splunk.Producer,
                 sid: "SID-to-consume"}
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

The above configuration assumes that Splunk credentials have been set up in your environment, either by having the
`SPLUNK_BASE_URL` and `SPLUNK_API_TOKEN` environment variables set, or by passing them directly to the producer.

```
...
producer: [
 module: {
   OffBroadway.Splunk.Producer,
   sid: "SID-to-consume",
   config: [
     base_url: "https://splunk.example.com",
     api_token: "my-secret-splunk-token"
   ]
 }
],
...
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
  def handle_batch(_, messages, _) do
    IO.puts("Received a batch of messages!")
    messages
  end
end
```

For the sake of the example, we're not really doing anything here. Whenever we're receiving a batch of messages, we just prints out a
message saying "Received a batch of messages!", and for each message we run `Message.update_data/2` passing a function that can process
that message ie. by doing some calculations on the data or something else.
