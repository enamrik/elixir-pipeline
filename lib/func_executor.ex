defmodule ElixirPipeline.FuncExecutor do
  defstruct [:func, :input_names, :output_names]

  @type result_type :: :ok|{:ok, any}|{:error, any}

  def build(func, input_names, output_names) do
    %__MODULE__{func: func, input_names: input_names, output_names: output_names}
  end

  def exec(%__MODULE__{func: func, input_names: input_names} = executor, prop, options \\ []) do
    inputs =
      case input_names do
        []    -> [single: prop]
        _else ->
          prop = prop || %{}
          input_names
          |> Enum.reverse()
          |> Enum.reduce(%{}, fn input_name, inputs -> inputs |> Map.put(input_name, prop[input_name])  end)
      end

    call_func_with_inputs = fn func, inputs ->
      arity = :erlang.fun_info(func)[:arity]
      case arity do
        0 -> func.() |> parse_func_call_result()
        1 -> func.(inputs) |> parse_func_call_result()
        2 -> func.(Keyword.get(options, :prop_path), inputs) |> parse_func_call_result()
      end
    end

    return_value =
      case inputs do
        [single: input] -> call_func_with_inputs.(func, input)

        _else           -> if length(Map.keys(inputs)) == 0,
                              do:   func.() |> parse_func_call_result(),
                              else: call_func_with_inputs.(func, inputs)
      end

    case Keyword.get(options, :process_return, true) do
      true  -> executor |> process_return(prop, return_value)
      false -> return_value
    end
  end

  defp process_return(%__MODULE__{output_names: output_names}, prop, return_value) do
    case return_value do
      :ok           -> {:ok, prop}
      {:ok, output} -> final_output =
                         cond do
                           length(output_names) == 0 ->
                             output
                           length(output_names) == 1 ->
                             (prop || %{}) |> Map.put(Enum.at(output_names, 0), output)
                           true ->
                             output_names
                             |> Enum.reduce(prop,
                                  fn o_name, cur_prop ->
                                    cur_prop
                                    |> Map.put(o_name, Map.get(output, o_name)) end)
                         end
                       {:ok, final_output}
      {:error, error} ->  {:error, error}
    end
  end

  @spec parse_func_call_result(any) :: result_type
  defp parse_func_call_result(func_result) do
    case func_result do
      :ok             -> :ok
      {:ok,    value} -> {:ok, value}
      {:error, error} -> {:error, error}
      value           -> {:ok, value}
    end
  end
end

