defmodule ElixirPipeline.Pipeline do
  defstruct [:props,:halt]

  def new() do
    %__MODULE__{props: %{}, halt: nil}
  end

  def add_value(%__MODULE__{} = pipeline, prop_name, value) when is_atom(prop_name) do
    pipeline |> if_continue(&(&1 |> put_prop(prop_name, value)))
  end

  def stop_if(%__MODULE__{} = pipeline, func, options \\ []) do
    pipeline
    |> if_continue(fn %__MODULE__{} = pipeline ->
      case pipeline |> call_func(func, Keyword.get(options, :inputs, [])) do
        true  -> pipeline |> stop_pipeline
        false -> pipeline
      end
    end)
  end

  def add_step(%__MODULE__{} = pipeline, func, options \\ []) when is_function(func) do
    pipeline
    |> if_continue(fn %__MODULE__{} = pipeline ->
      output = pipeline
               |> call_func(func, Keyword.get(options, :inputs, []))
               |> map_success(fn return_value ->
                  output_name = options |> Keyword.get(:output, :no_return)
                  if output_name == :no_return,
                     do: %{},
                     else: %{output_name => return_value}
              end)

      case output do
        {:ok, output}   -> pipeline |> merge_props(output)
        {:error, error} -> pipeline |> put_error(error)
      end
    end)
  end

  def to_result(%__MODULE__{props: props, halt: halt}, output_names \\ []) do
    case halt do
      [error: error] -> {:error, error}
      _else          -> {:ok, build_result(props, output_names)}
    end
  end

  defp build_result(props, []), do: props
  defp build_result(props, output_names) do
    output_names
    |> Enum.reduce(%{},
         fn output_name, output ->
           output |> Map.put(output_name, props[output_name])
         end)
  end

  defp if_continue(%__MODULE__{halt: halt} = pipeline, func) do
    case halt do
      nil   -> func.(pipeline)
      _else -> pipeline
    end
  end

  defp call_func(%__MODULE__{props: props}, func, inputs) do
    inputs = build_inputs(inputs, props)
    (if length(inputs) == 0, do: func.(), else: func.(inputs))
  end

  defp merge_props(%__MODULE__{props: props} = pipeline, another_props) do %{pipeline | props: props |> Map.merge(another_props)} end
  defp put_prop(%__MODULE__{props: props} = pipeline, prop_name, value) do %{pipeline | props: props |> Map.put(prop_name, value)} end
  defp put_error(%__MODULE__{} = pipeline, error)                       do %{pipeline | halt: [error: error]} end
  defp stop_pipeline(%__MODULE__{} = pipeline)                          do %{pipeline | halt: :short_circuit} end

  defp map_success({:ok, result}, func), do: func.(result)
  defp map_success({:error, error}, _),  do: {:error, error}
  defp map_success(result, func), do: {:ok, func.(result)}

  defp build_inputs(input_names, props) do
    input_names
    |> Enum.reverse()
    |> Enum.reduce([], fn input_name, step_options -> step_options |> Keyword.put(input_name, props[input_name])  end)
  end

  def is_struct(map) do
    Map.has_key?(map, :__struct__)
  end
end
