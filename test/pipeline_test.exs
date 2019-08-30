defmodule ElixirPipeline.PipelineTest do
  alias ElixirPipeline.Pipeline
  use ExUnit.Case

  describe "Pipeline" do
    test "from_map: can create pipeline from map" do
      output = Pipeline.from_map(%{id: 1, name: "some name"})
               |> Pipeline.to_result()

      assert output == {:ok, %{id: 1, name: "some name"}}
    end

    test "add_value: can add value to pipeline" do
      output = Pipeline.new()
               |> Pipeline.add_value(:id, 1)
               |> Pipeline.to_result()

      assert output == {:ok, %{id: 1}}
    end

    test "add_step: can run a step in the pipeline" do
      output = Pipeline.new()
               |> Pipeline.add_value(:id, 1)
               |> Pipeline.add_step(fn %{id: id} -> id + 1 end, inputs: [:id], output: :id_plus_1)
               |> Pipeline.to_result()

      assert output == {:ok, %{id: 1, id_plus_1: 2}}
    end

    test "to_result: can build result set" do
      output = Pipeline.new()
               |> Pipeline.add_value(:id, 1)
               |> Pipeline.add_step(fn %{id: id} -> id + 1 end, inputs: [:id], output: :id_plus_1)
               |> Pipeline.to_result([:id_plus_1])

      assert output == {:ok, %{id_plus_1: 2}}
    end

    test "to_result: should return nil for missing keys" do
      output = Pipeline.new()
               |> Pipeline.add_value(:id, 1)
               |> Pipeline.to_result([:id_plus_1])

      assert output == {:ok, %{id_plus_1: nil}}
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
               |> Pipeline.stop_if(fn %{should_stop: should_stop} -> should_stop end, inputs: [:should_stop])
               |> Pipeline.add_step(fn-> 1 end, output: :id)
               |> Pipeline.to_result([:id])

      assert output == {:ok, %{id: nil}}
    end

    test "stop_if: continues pipeline on false" do
      output = Pipeline.new()
               |> Pipeline.add_value(:id, 1)
               |> Pipeline.stop_if(fn-> false end)
               |> Pipeline.add_step(fn %{id: id} -> id + 1 end, inputs: [:id], output: :id_plus_1)
               |> Pipeline.to_result()

      assert output == {:ok, %{id: 1, id_plus_1: 2}}
    end

    test "add_step: output is optional" do
      some_id = "someId"
      {:ok, agent_id} = Agent.start_link(fn -> nil end)

      output = Pipeline.new()
               |> Pipeline.add_value(:id, some_id)
               |> Pipeline.add_step(fn %{id: id} -> Agent.update(agent_id, fn _ -> id end) end, inputs: [:id])
               |> Pipeline.to_result([:id])

      assert Agent.get(agent_id, &(&1)) == some_id
      assert output == {:ok, %{id: some_id}}

      Agent.stop(agent_id)
    end

    test "add_step: supports multiple outputs" do
      output = Pipeline.new()
               |> Pipeline.add_step(fn-> {:ok, %{id: 1, name: "joe"}} end, outputs: [:id, :name])
               |> Pipeline.to_result([:id, :name])

      assert output == {:ok, %{id: 1, name: "joe"}}
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
               |> Pipeline.add_step(fn %{id: id} -> id + 1 end, inputs: [:id], output: :id_plus_1)
               |> Pipeline.to_result([:id_plus_1])

      assert output == {:error, "someError"}
    end

    test "add_step: can call step function with no inputs" do
      output = Pipeline.new()
               |> Pipeline.add_step(fn-> 1 end, output: :id)
               |> Pipeline.to_result([:id])

      assert output == {:ok, %{id: 1}}
    end

    test "add_step: can handle nil value" do
      {:ok, output} = Pipeline.new()
                      |> Pipeline.add_value(:event, nil)
                      |> Pipeline.add_step(fn %{event: _event} ->  "new_value" end, inputs: [:event], outputs: [:event])
                      |> Pipeline.to_result()

      assert output[:event] == "new_value"
    end

    test "add_step: can handle nil nested value" do
      event = %{id: "1", name: "some name"}
      {:ok, output} = Pipeline.new()
                      |> Pipeline.add_value(:event, event)
                      |> Pipeline.add_step(fn %{name: _name} ->  "new_value" end, with: :event, inputs: [:name], outputs: [:name])
                      |> Pipeline.to_result()

      assert output[:event] == %{id: "1", name: "new_value"}
    end

    test "add_step: can handle missing nested value" do
      event = %{id: "1", name: "some name"}
      {:ok, output} = Pipeline.new()
                      |> Pipeline.add_value(:event, event)
                      |> Pipeline.add_step(fn %{street: _street} ->  "new_value" end,
                           with: [:event, :address], inputs: [:street], outputs: [:street])
                      |> Pipeline.to_result()

      assert output[:event] == %{id: "1", name: "some name", address: %{street: "new_value"}}
    end

    test "add_step: can transform one property of an object" do
      event = %{id: "1", name: "some name"}
      {:ok, output} = Pipeline.new()
                      |> Pipeline.add_value(:event, event)
                      |> Pipeline.add_step(fn %{name: name} ->  "#{name} new" end, with: :event, inputs: [:name], outputs: [:name])
                      |> Pipeline.to_result()

      assert output[:event] == %{id: "1", name: "some name new"}
    end

    test "add_step: can transform properties of an object" do
      event = %{id: "1", name: "some name"}
      {:ok, output} = Pipeline.new()
                      |> Pipeline.add_value(:event, event)
                      |> Pipeline.add_step(
                           fn %{id: id, name: name} ->  %{id: "#{id} new", name: "#{name} new"} end,
                           with: :event,
                           inputs: [:id, :name],
                           outputs: [:id, :name])
                      |> Pipeline.to_result()

      assert output[:event] == %{id: "1 new", name: "some name new"}
    end

    test "add_step: can transform primitive properties of an object" do
      event = %{id: "1", name: "some name"}
      {:ok, output} = Pipeline.new()
                      |> Pipeline.add_value(:event, event)
                      |> Pipeline.add_step(
                           fn name ->  "#{name} new" end,
                           with: [:event, :name])
                      |> Pipeline.to_result()

      assert output[:event] == %{id: "1", name: "some name new"}
    end

    test "add_step: can transform properties of a nested object" do
      event = %{id: "1", address: %{street: "some street", zip: "123"}}
      {:ok, output} = Pipeline.new()
                      |> Pipeline.add_value(:event, event)
                      |> Pipeline.add_step(
                           fn %{street: street, zip: zip} ->  %{street: "#{street} new", zip: "#{zip} new"} end,
                           with: [:event, :address],
                           inputs: [:street, :zip],
                           outputs: [:street, :zip])
                      |> Pipeline.to_result()

      assert output[:event] == %{id: "1", address: %{street: "some street new", zip: "123 new"}}
    end

    test "add_step: can transform property of a nested object" do
      event = %{id: "1", address: %{street: "some street", zip: "123"}}
      {:ok, output} = Pipeline.new()
                      |> Pipeline.add_value(:event, event)
                      |> Pipeline.add_step(
                           fn %{street: street, zip: zip} ->  "#{street}-#{zip} new" end,
                           with: [:event, :address],
                           inputs: [:street, :zip],
                           outputs: [:street])
                      |> Pipeline.to_result()

      assert output[:event] == %{id: "1", address: %{street: "some street-123 new", zip: "123"}}
    end

    test "add_step: can transform properties of an object with string keys" do
      event = %{"id" => "1", "name" => "some name"}
      {:ok, output} = Pipeline.new()
                      |> Pipeline.add_value("event", event)
                      |> Pipeline.add_step(
                           fn %{"id" => id, "name" => name} -> %{"id" => "#{id} new", "name" => "#{name} new"} end,
                           with: "event",
                           inputs: ["id", "name"],
                           outputs: ["id", "name"])
                      |> Pipeline.to_result()

      assert output["event"] == %{"id" => "1 new", "name" => "some name new"}
    end


    test "add_step: can transform nested arrays of objects" do
      event = %{id: "1",
        addresses: [
          %{street: "some street", zip: %{id: "123"}},
          %{street: "some street 2", zip: %{id: "345"}}]}

      {:ok, output} = Pipeline.new()
                      |> Pipeline.add_value(:event, event)
                      |> Pipeline.add_step(
                           fn %{id: zip_id} -> "#{zip_id} new" end,
                           with: [:event, [each: :addresses], :zip],
                           inputs: [:id],
                           outputs: [:id])
                      |> Pipeline.to_result()

      assert output[:event] == %{id: "1",
               addresses: [
                 %{street: "some street", zip: %{id: "123 new"}},
                 %{street: "some street 2", zip: %{id: "345 new"}}]}
    end

    test "add_step: can transform nested arrays of object props" do
      event = %{id: "1",
        addresses: [
          %{street: "some street", zip: %{id: "123"}},
          %{street: "some street 2", zip: %{id: "345"}}]}

      {:ok, output} = Pipeline.new()
                      |> Pipeline.add_value(:event, event)
                      |> Pipeline.add_step(
                           fn zip_id ->  "#{zip_id} new" end,
                           with: [:event, [each: :addresses], :zip, :id])
                      |> Pipeline.to_result()

      assert output[:event] == %{id: "1",
               addresses: [
                 %{street: "some street", zip: %{id: "123 new"}},
                 %{street: "some street 2", zip: %{id: "345 new"}}]}
    end

    test "add_step: can transform nested arrays" do
      event = %{id: "1", addresses: [ "some street", "some street 2"]}

      {:ok, output} = Pipeline.new()
                      |> Pipeline.add_value(:event, event)
                      |> Pipeline.add_step(
                           fn address ->  "#{address} new" end,
                           with: [:event, [each: :addresses]])
                      |> Pipeline.to_result()

      assert output[:event] == %{id: "1", addresses: [ "some street new", "some street 2 new"]}
    end

    test "add_step: can collect errors from multiple step failures" do

      event = %{
        id: "1",
        name: nil,
        address: %{street: "some street", zip: nil},
        attendees: [%{name: nil}, %{address: %{street: "some street", zip: nil}}]}

      {:error, error} = Pipeline.new()
                        |> Pipeline.add_value(:id, nil)
                      |> Pipeline.add_value(:event, event)
                        |> Pipeline.add_step(
                             fn %{id: _id} -> {:error, "Id cannot be blank"} end,
                             inputs: [:id],
                             collect: :error)
                      |> Pipeline.add_step(
                           fn %{name: _name} -> {:error, "Name cannot be blank"} end,
                           with: :event,
                           inputs: [:name],
                           collect: :error)
                      |> Pipeline.add_step(
                           fn %{street: _street} -> {:error, "Street cannot be blank"} end,
                           with: [:event, :address],
                           inputs: [:street],
                           collect: :error)
                        |> Pipeline.add_step(
                             fn %{name: _name} -> {:error, "Attendee name cannot be blank"} end,
                             with: [:event, [each: :attendees], :name],
                             inputs: [:name],
                             collect: :error)
                        |> Pipeline.add_step(
                             fn name, %{zip: _zip} -> {:error, "#{inspect(name)} cannot be blank"} end,
                             with: [:event, [each: :attendees], :address],
                             inputs: [:zip],
                             collect: :error)
                      |> Pipeline.add_step(
                           fn name, _zip -> {:error, "#{name |> Enum.join(".")} cannot be blank"} end,
                           with: [:event, :address, :zip],
                           collect: :error_end)
                      |> Pipeline.to_result()

      assert error == [
               "Id cannot be blank",
               "Name cannot be blank",
               "Street cannot be blank",
               "Attendee name cannot be blank",
               "Attendee name cannot be blank",
               "[:event, :attendees, 0, :address] cannot be blank",
               "[:event, :attendees, 1, :address] cannot be blank",
               "event.address.zip cannot be blank"]
    end
  end
end
