defmodule OffBroadwaySplunk.ApiClient do
  @moduledoc """
  API Client for Splunk.
  """

  def client do
    middleware = [
      {Tesla.Middleware.BaseUrl, client_option(:base_url)},
      {Tesla.Middleware.BearerAuth, token: client_option(:api_token)},
      {Tesla.Middleware.JSON, engine: Jason}
    ]

    Tesla.client(middleware)
  end

  @spec search_jobs(Tesla.Client.t()) :: {:ok, Tesla.Env.t()}
  def search_jobs(client),
    do: client |> Tesla.get("/services/search/jobs", query: [output_mode: "json"])

  @spec search_jobs(Tesla.Client.t(), String.t()) :: {:ok, Tesla.Env.t()}
  def search_jobs(client, sid) when is_binary(sid),
    do: client |> Tesla.get("/services/search/jobs/#{sid}", query: [output_mode: "json"])

  @spec search_results(Tesla.Client.t(), String.t()) :: {:ok, Tesla.Env.t()}
  def search_results(client, sid) do
    # TODO - When Tesla supports streaming responses, we should probably replace
    # this with `output_mode: "raw"` and stream all events in one go. Until that is
    # in place we might as well use `output_mode: "json"`, since it's easier to parse.
    #
    # For now, we need to use `offset` and `count` and do multiple requests in order
    # to avoid having to large payloads at the time.
    # The Broadway producer should probably be in charge of `count` attribute and match it to
    # the current demand. Number of workers (which controls `offset`) depends on the number
    # of producers we run at any given time. This needs to be synced in order to avoid pulling
    # the same records multiple times.
    client
    |> Tesla.get("/services/search/jobs/#{sid}/results", query: [output_mode: "json"])
  end

  @spec client_option(Atom.t()) :: String.t()
  defp client_option(opt) when is_atom(opt) do
    with opts <- Application.get_env(:off_broadway_splunk, :api_client),
         value when not is_nil(value) <- Keyword.get(opts, opt),
         do: value
  end
end
