# OffBroadway.Splunk

[![pipeline status](https://gitlab.intility.com/soc/off_broadway_splunk/badges/master/pipeline.svg)](https://gitlab.intility.com/soc/off_broadway_splunk/-/commits/master)
[![coverage report](https://gitlab.intility.com/soc/off_broadway_splunk/badges/master/coverage.svg)](https://gitlab.intility.com/soc/off_broadway_splunk/-/commits/master)

A Splunk event consumer for [Broadway](https://github.com/dashbitco/broadway).

Read the full documentation [here](http://soc.pages.intility.com/off_broadway_splunk)!

## Installation

This package is not yet available in [Hex](https://hex.pm/docs/publish), so it must be installed
directly from the [Intility Gitlab](https://gitlab.intility.com) server by adding `off_broadway_splunk` to your list of dependencies in `mix.exs`.

```elixir
def deps do
  [
    {:off_broadway_splunk, git: "git@gitlab.intility.com:soc/off_broadway_splunk.git", tag: "0.1.0"}
  ]
end
```
