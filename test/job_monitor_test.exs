defmodule OffBroadwaySplunk.JobMonitorTest do
  use ExUnit.Case, async: false

  import Tesla.Mock
  alias OffBroadwaySplunk.JobMonitor

  @sid_complete "SID-1"
  @sid_incomplete "SID-2"
  @sid_zombie "SID-3"
  @sid_not_found "SID-4"
  @sid_bad_request "SID-5"

  describe "OffBroadwaySplunk.JobMonitor" do
    setup do
      orig_config = Application.get_env(:off_broadway_splunk, :api_client)

      Application.put_env(:off_broadway_splunk, :api_client,
        base_url: "https://splunk.example.com"
      )

      {:ok, pid1} = GenServer.start_link(JobMonitor, %{sid: @sid_complete})
      {:ok, pid2} = GenServer.start_link(JobMonitor, %{sid: @sid_incomplete})
      {:ok, pid3} = GenServer.start_link(JobMonitor, %{sid: @sid_zombie})
      {:ok, pid4} = GenServer.start_link(JobMonitor, %{sid: @sid_not_found})
      {:ok, pid5} = GenServer.start_link(JobMonitor, %{sid: @sid_bad_request})

      mock_global(fn
        %{method: :get, url: "https://splunk.example.com/services/search/jobs/SID-1"} ->
          %Tesla.Env{
            status: 200,
            body: %{
              "entry" => [
                %{
                  "sid" => "SID-1",
                  "published" => DateTime.to_iso8601(DateTime.utc_now()),
                  "content" => %{
                    "isZombie" => false,
                    "isDone" => true,
                    "eventCount" => 25_000,
                    "doneProgress" => 1
                  }
                }
              ]
            }
          }

        %{method: :get, url: "https://splunk.example.com/services/search/jobs/SID-2"} ->
          %Tesla.Env{
            status: 200,
            body: %{
              "entry" => [
                %{
                  "sid" => "SID-2",
                  "published" => DateTime.to_iso8601(DateTime.utc_now()),
                  "content" => %{
                    "isZombie" => false,
                    "isDone" => false,
                    "eventCount" => 25_000,
                    "doneProgress" => 0.5
                  }
                }
              ]
            }
          }

        %{method: :get, url: "https://splunk.example.com/services/search/jobs/SID-3"} ->
          %Tesla.Env{
            status: 200,
            body: %{
              "entry" => [
                %{
                  "sid" => "SID-3",
                  "published" => DateTime.to_iso8601(DateTime.utc_now()),
                  "content" => %{
                    "isZombie" => true,
                    "isDone" => false,
                    "eventCount" => 25_000,
                    "doneProgress" => 0.5
                  }
                }
              ]
            }
          }

        %{method: :get, url: "https://splunk.example.com/services/search/jobs/SID-4"} ->
          %Tesla.Env{status: 404, body: ""}

        %{method: :get, url: "https://splunk.example.com/services/search/jobs/SID-5"} ->
          %Tesla.Env{status: 400}
      end)

      on_exit(fn ->
        Application.put_env(:off_broadway_splunk, :api_client, orig_config)
        if Process.alive?(pid1), do: GenServer.stop(pid1, :normal)
        if Process.alive?(pid2), do: GenServer.stop(pid2, :normal)
        if Process.alive?(pid3), do: GenServer.stop(pid3, :normal)
        if Process.alive?(pid4), do: GenServer.stop(pid4, :normal)
        if Process.alive?(pid5), do: GenServer.stop(pid5, :normal)
      end)

      {:ok, pid1: pid1, pid2: pid2, pid3: pid3, pid4: pid4, pid5: pid5}
    end

    test "process can be started", %{pid1: pid} do
      assert Process.alive?(pid)
    end

    test "state can be created" do
      assert %JobMonitor.State{
               sid: @sid_complete
             } = JobMonitor.State.new(%{"sid" => @sid_complete})
    end

    test "job complete processing state", %{pid1: pid} do
      send(pid, :tick)

      assert %{
               is_done: true,
               sid: @sid_complete
             } = GenServer.call(pid, :get_state)
    end

    test "job incomplete state", %{pid2: pid} do
      send(pid, :tick)

      assert %{
               is_done: false,
               sid: @sid_incomplete
             } = GenServer.call(pid, :get_state)
    end

    test "job zombie state", %{pid3: pid} do
      send(pid, :tick)

      assert %{
               is_zombie: true,
               sid: @sid_zombie
             } = GenServer.call(pid, :get_state)
    end

    test "receives error response from Splunk API", %{pid4: pid4, pid5: pid5} do
      send(pid4, :tick)
      send(pid5, :tick)

      Process.sleep(200)

      refute Process.alive?(pid4)
      refute Process.alive?(pid5)
    end
  end
end
