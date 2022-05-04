# OffBroadwaySplunk

A Splunk event consumer for [Broadway](https://github.com/dashbitco/broadway).

Broadway producer acts as a consumer for the specified Splunk SID (Search ID).

## Installation

This package is not yet available in [Hex](https://hex.pm/docs/publish), so it must be installed
directly from the [Intility Gitlab](https://gitlab.intility.com) server.
This can be achieved by adding `off_broadway_splunk` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:off_broadway_splunk, git: "git@gitlab.intility.com:soc/constream.git", tag: "0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/off_broadway_splunk>.
