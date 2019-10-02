defmodule ElixirPipeline.LogStepTest do
  alias ElixirPipeline.Pipeline

  use ExUnit.Case

  describe "Pipeline-LogStep" do
    @tag :one
    test "can log step" do
      pipeline_id = "c7ae6d13-5570-4cb6-844c-2a67b89cda8e"
      some_id = "someId"
      TestLogger.start_link()

      Pipeline.new(
        id: pipeline_id,
        log_level: :info,
        logger: %{info:  &TestLogger.info/1,
                  debug: &TestLogger.debug/1,
                  warn:  &TestLogger.warn/1,
                  error: &TestLogger.error/1}
      )
      |> Pipeline.add_value(:id, some_id)
      |> Pipeline.add_step(fn %{id: id} -> %{id: "#{id}_plus", name: "someName"} end, inputs: [:id], outputs: [:id, :name])
      |> Pipeline.to_result([:id])

      assert TestLogger.get_logs() == ["step:1:( id -> id,name ):pipeline(#{pipeline_id}):success"]
      TestLogger.stop()
    end
  end
end

defmodule TestLogger do
  def start_link() do
    Agent.start_link(fn -> [] end, name: __MODULE__)
  end

  def get_logs() do
    Agent.get(__MODULE__, &(&1))
  end

  def stop() do
    Agent.stop(__MODULE__)
  end

  def debug(text) do
    Agent.update(__MODULE__, &([text | &1 ]))
  end

  def info(text) do
    Agent.update(__MODULE__, &([text | &1 ]))
  end

  def warn(text) do
    Agent.update(__MODULE__, &([text | &1 ]))
  end

  def error(text) do
    Agent.update(__MODULE__, &([text | &1 ]))
  end
end