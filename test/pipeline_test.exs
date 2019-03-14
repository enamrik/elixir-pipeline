defmodule ElixirPipeline.PipelineTest do
  alias ElixirPipeline.Pipeline
  use ExUnit.Case

  describe "Pipeline" do
    test "add_value: can add value to pipeline" do
      output = Pipeline.new()
               |> Pipeline.add_value(:id, 1)
               |> Pipeline.to_result()

      assert output == {:ok, %{id: 1}}
    end

    test "add_step: can run a step in the pipeline" do
      output = Pipeline.new()
               |> Pipeline.add_value(:id, 1)
               |> Pipeline.add_step(fn [id: id] -> id + 1 end, inputs: [:id], output: :id_plus_1)
               |> Pipeline.to_result()

      assert output == {:ok, %{id: 1, id_plus_1: 2}}
    end

    test "to_result: can build result set" do
      output = Pipeline.new()
               |> Pipeline.add_value(:id, 1)
               |> Pipeline.add_step(fn [id: id] -> id + 1 end, inputs: [:id], output: :id_plus_1)
               |> Pipeline.to_result([:id_plus_1])

      assert output == {:ok, %{id_plus_1: 2}}
    end

    test "to_result: should return nil for missing keys" do
      output = Pipeline.new()
               |> Pipeline.add_value(:id, 1)
               |> Pipeline.to_result([:id_plus_1])

      assert output == {:ok, %{id_plus_1: nil}}
    end

    test "add_step: output is optional" do
      some_id = "someId"
      {:ok, agent_id} = Agent.start_link(fn -> nil end)

      output = Pipeline.new()
               |> Pipeline.add_value(:id, some_id)
               |> Pipeline.add_step(fn [id: id] -> Agent.update(agent_id, fn _ -> id end) end, inputs: [:id])
               |> Pipeline.to_result([:id])

      assert Agent.get(agent_id, &(&1)) == some_id
      assert output == {:ok, %{id: some_id}}

      Agent.stop(agent_id)
    end

    test "add_step: output is optional when step returns elixir result format" do
      some_id = "someId"
      {:ok, agent_id} = Agent.start_link(fn -> nil end)

      output = Pipeline.new()
               |> Pipeline.add_value(:id, some_id)
               |> Pipeline.add_step(fn [id: id] -> {:ok, Agent.update(agent_id, fn _ -> id end)} end, inputs: [:id])
               |> Pipeline.to_result([:id])

      assert Agent.get(agent_id, &(&1)) == some_id
      assert output == {:ok, %{id: some_id}}

      Agent.stop(agent_id)
    end

    test "add_step: can override value" do
      output = Pipeline.new()
               |> Pipeline.add_value(:id, 1)
               |> Pipeline.add_step(fn _ -> 2 end, inputs: [:id], output: :id)
               |> Pipeline.to_result([:id])

      assert output == {:ok, %{id: 2}}
    end

    test "add_step: will short circuit on error" do
      output = Pipeline.new()
               |> Pipeline.add_value(:id, 1)
               |> Pipeline.add_step(fn _ -> {:error, "someError"} end, inputs: [:id])
               |> Pipeline.add_step(fn [id: id] -> id + 1 end, inputs: [:id], output: :id_plus_1)
               |> Pipeline.to_result([:id_plus_1])

      assert output == {:error, "someError"}
    end

    test "add_step: can call step function with no inputs" do
      output = Pipeline.new()
               |> Pipeline.add_step(fn-> 1 end, output: :id)
               |> Pipeline.to_result([:id])

      assert output == {:ok, %{id: 1}}
    end

    test "stop_if: should stop pipeline if evaluates to true" do
      output = Pipeline.new()
               |> Pipeline.stop_if(fn-> true end)
               |> Pipeline.add_step(fn-> 1 end, output: :id)
               |> Pipeline.to_result()

      assert output == {:ok, %{}}
    end

    test "stop_if: can take inputs" do
      output = Pipeline.new()
               |> Pipeline.add_value(:should_stop, true)
               |> Pipeline.stop_if(fn [should_stop: should_stop]-> should_stop end, inputs: [:should_stop])
               |> Pipeline.add_step(fn-> 1 end, output: :id)
               |> Pipeline.to_result([:id])

      assert output == {:ok, %{id: nil}}
    end

    test "stop_if: continues pipeline on false" do
      output = Pipeline.new()
               |> Pipeline.add_value(:id, 1)
               |> Pipeline.stop_if(fn-> false end)
               |> Pipeline.add_step(fn [id: id] -> id + 1 end, inputs: [:id], output: :id_plus_1)
               |> Pipeline.to_result()

      assert output == {:ok, %{id: 1, id_plus_1: 2}}
    end
  end
end
