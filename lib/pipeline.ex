defmodule ElixirPipeline.Pipeline do
  defstruct [:props,:halt]

  @type t :: %__MODULE__{props: map, halt: any}
  @type result_type :: :ok|{:ok, any}|{:error, any}
  @type action :: (-> :ok|{:ok, any}|{:error, any})|(map -> :ok|{:ok, any}|{:error, any})

  @spec new() :: __MODULE__.t()
  def new() do
    %__MODULE__{props: %{}, halt: nil}
  end

  @spec add_value(__MODULE__.t(), atom(), any()) :: __MODULE__.t()
  def add_value(%__MODULE__{} = pipeline, prop_name, value) when is_atom(prop_name) do
    pipeline |> if_continue(&(&1 |> put_prop(prop_name, value)))
  end

  @spec stop_if(__MODULE__.t(), (-> boolean)|(map -> boolean), [{:inputs, [atom]}]) :: __MODULE__.t()
  def stop_if(%__MODULE__{} = pipeline, func, options \\ []) do
    pipeline
    |> if_continue(fn %__MODULE__{} = pipeline ->
      case pipeline |> call_func(Keyword.get(options, :inputs, []), func) do
        {:ok,  true}  -> pipeline |> stop_pipeline
        _else         -> pipeline
      end
    end)
  end

  @spec add_step(__MODULE__.t(), action, [{:inputs, [atom]}, {:output, atom}]) :: __MODULE__.t()
  def add_step(%__MODULE__{} = pipeline, func, options \\ []) when is_function(func) do
    pipeline
    |> if_continue(fn %__MODULE__{} = pipeline ->
      output = pipeline
               |> call_func(Keyword.get(options, :inputs, []), func)
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

  @spec to_result(__MODULE__.t(), any()) :: {:error, any()} | {:ok, any()}
  def to_result(%__MODULE__{props: props, halt: halt}, output_names \\ []) do
    case halt do
      [error: error] -> {:error, error}
      _else          -> {:ok, build_result(props, output_names)}
    end
  end

  @spec build_result(map, [atom]) :: map
  defp build_result(props, output_names) do
    if length(output_names) == 0,
    do:   props,
    else: output_names
          |> Enum.reduce(%{},
              fn output_name, output ->
                output |> Map.put(output_name, props[output_name])
              end)
  end

  @spec if_continue(__MODULE__.t(), (__MODULE__.t() -> __MODULE__.t())) :: __MODULE__.t()
  defp if_continue(%__MODULE__{halt: halt} = pipeline, func) do
    case halt do
      nil   -> func.(pipeline)
      _else -> pipeline
    end
  end

  @spec call_func(__MODULE__.t(), [atom], (-> any) | (map -> any)) :: result_type
  defp call_func(%__MODULE__{props: props}, inputs, func) do
    inputs = build_inputs(inputs, props)
    if length(inputs) == 0,
    do:   func.() |> handle_result(),
    else: func.(inputs) |> handle_result()
  end

  @spec merge_props(__MODULE__.t, map) :: __MODULE__.t
  defp merge_props(%__MODULE__{props: props} = pipeline, another_props) do %{pipeline | props: props |> Map.merge(another_props)} end

  @spec put_prop(__MODULE__.t, atom, any) :: __MODULE__.t
  defp put_prop(%__MODULE__{props: props} = pipeline, prop_name, value) do %{pipeline | props: props |> Map.put(prop_name, value)} end

  @spec put_error(__MODULE__.t, any) :: __MODULE__.t
  defp put_error(%__MODULE__{} = pipeline, error) do %{pipeline | halt: [error: error]} end

  @spec stop_pipeline(__MODULE__.t) :: __MODULE__.t
  defp stop_pipeline(%__MODULE__{} = pipeline) do %{pipeline | halt: :short_circuit} end

  @spec map_success(result_type, (any -> result_type)) :: result_type
  defp map_success(result, func) do
    case result do
      :ok             -> func.(nil) |> handle_result()
      {:ok,    value} -> func.(value) |> handle_result()
      {:error, error} -> {:error, error}
    end
  end

  @spec build_inputs([atom], map) :: [any]
  defp build_inputs(input_names, props) do
    input_names
    |> Enum.reverse()
    |> Enum.reduce([], fn input_name, step_options -> step_options |> Keyword.put(input_name, props[input_name])  end)
  end

  @spec handle_result(any) :: result_type
  defp handle_result(func_result) do
    case func_result do
      :ok             -> :ok
      {:ok,    value} -> {:ok, value}
      {:error, error} -> {:error, error}
      value           -> {:ok, value}
    end
  end
end
