defmodule OffBroadway.Splunk.MyBroadway do
  use Broadway

  alias Broadway.Message

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        concurrency: 2,
        module: {
          OffBroadway.Splunk.Producer,
          sid:
            "scheduler__aa646__search__RMD5cd37bc9103ca4b41_at_1656421200_62928_8CB53D79-587A-43EE-95CC-14256C65EF95",
          receive_interval: 5000
          # config: [api_token: "foo", base_url: "https://splunk.example.com"]
        }
      ],
      processors: [
        default: [
          # concurrency: 2,
          min_demand: 500,
          max_demand: 1000
        ]
      ]
      # batchers: [
      #   default: [
      #     batch_size: 100,
      #     batch_timeout: 2000
      #   ]
      # ]
    )
  end

  @impl true
  def handle_message(_processor, %Message{data: _data} = message, _context) do
    # IO.puts("Processor #{inspect(self())}: received message!")
    message
  end

  @impl true
  def handle_batch(_, messages, _, _) do
    IO.puts("Processor #{inspect(self())} - Processing batch with #{length(messages)} messages.")
    messages
  end
end
