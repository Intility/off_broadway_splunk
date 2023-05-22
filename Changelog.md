# Changelog

## 2.0.0 - Upgrade version

_Unreleased_

This almost a complete rewrite and is **uncompatible** with the `v1.x` branch.
Instead of targeting a specific `SID` to produce messages for, this release is focused around producing messages
from Splunk Reports or (triggered) Alerts.
This is a more efficient way to prepare data for export by Splunk, and produces more predictable messages both
in terms of when they are available and the structure of the data in the messages.

Instead of passing a `SID` to the producer option, simply pass the `name` for your report or alert. The
`OffBroadway.Splunk.Queue` process will query the Splunk Web API and fetch a list of available jobs that can produce
messages for the given report or alert.

Options

- Replace `sid` option with `name`. Pipelines should now be given the name of a report or alert.
- Remove `endpoint` option. All messages will be downloaded using the `results` endpoint.
- Remove `offset` option, as it is only available for the `events` endpoint.
- Add `refetch_interval`option. This is the amount in milliseconds the `OffBroadway.Splunk.Queue` process will
  wait before refetching the list of available jobs.

Other

- Add `OffBroadway.Splunk.Queue` GenServer process that will start as part of the pipeline supervision tree.
- Remove `OffBroadway.Splunk.Leader` GenServer process as it is not usable anymore.
- Refactored `OffBroadway.Splunk.Producer` and `OffBroadway.Splunk.SplunkClient` to new workflow.
- Updated `telemetry` events to new workflow.

## 1.2.4 - Bug fix

_Released 2023-04-20_

Bug fixes

- Using `state.is_done` proved unreliable when consuming certain jobs. Replaced calculation of retry timings
  to be based on `receive_interval`.
- Fixed typings for `OffBroadway.Splunk.Leader` struct.

## 1.2.3 - Minor fixes

_Released 2023-04-05_

Minor fixes

- Remove `Tesla.Middleware.Logger` from default `OffBroadway.Splunk.SplunkClient` tesla client because
  of too much noise.

## 1.2.2 - Minor fixes

_Released 2023-04-03_

Minor fixes

- Filter `authorization` headers for `Tesla.Middleware.Logger`
- Replace some enumerations with streams

## 1.2.1 - Dependecies

_Released 2023-03-28_

Upgrade accepted dependencies

- Accept `nimble_options` version `v1.0`

## 1.2.0 - Feature release

_Released 2023-01-23_

Added new options

- `api_version` - Configures if messages should be produced from the `v1` or `v2` versioned API endpoints.

Upgrade accepted dependencies

- Accept `telemetry` version `1.1` or `1.2`
- Accept `tesla` version `1.4` or `1.5`

## 1.1.1 - Feature release

_Released 2023-01-16_

Added new options

- `shutdown_timeout` - Configurable number of milliseconds Broadway should wait before timing out when trying to stop
  the pipeline.
- `endpoint` - Choose to consume messages using the `events` or `results` endpoint of the Splunk Web API.
- `offset` - Allow passing a custom initial offset to start consuming messages from. Passing a negative value will
  cause the pipeline to consume messages from the "end" of the results.
- `max_events` - If set to a positive integer, shut down the pipeline after producing this many messages.

## 1.1.0 - Initial release

_Released 2022-10-28_

The first release targeted consuming a single SID (Search ID) produced by saving a triggered alert.
