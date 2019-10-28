defmodule ElixirPipeline.Pipeline do
  defstruct [:props,:halt, :collected_errors, :steps_info, :id, :name, :log_level, :logger]

  @type t :: %__MODULE__{props: map, halt: any, collected_errors: list, steps_info: map, id: String.t, log_level: atom}
  @type result_type :: :ok|{:ok, any}|{:error, any}
  @type action :: (-> :ok|{:ok, any}|{:error, any})|(map -> :ok|{:ok, any}|{:error, any})

  import ElixirPipeline.AtomizeKeys, only: [atomize_keys: 1]
  alias ElixirPipeline.FuncExecutor
  require Logger

  @spec new() :: __MODULE__.t()
  def new(options \\ []) do
    name      = Keyword.get(options, :name, "pipeline")
    id        = Keyword.get(options, :id)
    log_level = Keyword.get(options, :log_level, :debug)
    logger    = Keyword.get(options, :logger, %{info:  &Logger.info/1,
                                                debug: &Logger.debug/1,
                                                warn:  &Logger.warn/1,
                                                error: &Logger.error/1})
    %__MODULE__{
      props: %{},
      halt: nil,
      collected_errors: [],
      steps_info: %{count: 0},
      id: id || UUID.uuid4(),
      name: name,
      log_level: log_level,
      logger: logger}
  end

  @spec from_map(map, [{:atomize_keys, bool}]) :: __MODULE__.t()
  def from_map(map, options \\ []) do
    map = if Keyword.get(options, :atomize_keys, false), do:  atomize_keys(map), else: map
    %{new(options) | props: map}
  end

  @spec add_value(__MODULE__.t(), atom | binary, any()) :: __MODULE__.t()
  def add_value(%__MODULE__{} = pipeline, prop_name, value) do
    pipeline |> if_continue(&(&1 |> put_prop(prop_name, value)))
  end

  @spec stop_if(__MODULE__.t(), (-> boolean)|(map -> boolean), [{:inputs, [atom | binary]}]) :: __MODULE__.t()
  def stop_if(%__MODULE__{props: props} = pipeline, func, options \\ []) do
    pipeline
    |> if_continue(fn %__MODULE__{} = pipeline ->
      return_value = FuncExecutor.build(func, Keyword.get(options, :inputs, []), [])
                     |> FuncExecutor.exec(props, process_return: false)
      case return_value do
        {:ok,  true}  -> pipeline |> stop_pipeline
        _else         -> pipeline
      end
    end)
  end

  @spec add_step(__MODULE__.t(), action, [
    {:inputs, [atom | binary]},
    {:output, atom | binary},
    {:outputs, [atom | binary]},
    {:with, atom | binary | [atom] | [binary]}]) :: __MODULE__.t()
  def add_step(%__MODULE__{props: props} = pipeline, func, options \\ []) when is_function(func) do
    pipeline
    |> if_continue(fn %__MODULE__{} = pipeline ->

      prop_path    = get_prop_path(options)
      input_names  = Keyword.get(options, :inputs, [])
      output_names = get_output_names(options)

      result = apply_in(props, prop_path, nil, func, input_names, output_names)

      case result do
        {:ok, updated_props} -> %{ pipeline | props: updated_props}
        {:error,      error} -> case Keyword.get(options, :collect) do
                                  :error     -> pipeline |> collect_error(error)
                                  _else      -> pipeline |> put_error(error)
                                end
      end
      |> log_step_info(Keyword.get(options, :name), input_names, output_names, result)
    end)
  end

  def collect_error_to_error(%__MODULE__{collected_errors: collected_errors} = pipeline) do
    pipeline
    |> if_continue(fn %__MODULE__{} = pipeline ->
      case collected_errors do
        []    -> pipeline
        _else -> %{pipeline | collected_errors: []} |> put_error(collected_errors)
      end
    end)
  end

  def print(%__MODULE__{} = pipeline, prefix \\ "Pipeline:") do
    IO.puts("#{prefix}#{inspect(pipeline)}")
    pipeline
  end

  defp apply_in(props, path, last_prop_path, func, input_names, output_names) do

    cur_prop_name   = path |> Enum.at(0)
    remaining_names = path |> Enum.drop(1)
    last_prop_path  = last_prop_path || []

    case cur_prop_name do
      nil                    -> FuncExecutor.build(func, input_names, output_names)
                                |> FuncExecutor.exec(props, prop_path: last_prop_path)
      [each: list_prop_name] -> (props[list_prop_name] || [])
                                |> Enum.with_index()
                                |> Enum.map(fn {prop, index} -> fn-> apply_in(prop, remaining_names, last_prop_path ++ [list_prop_name] ++ [index], func, input_names, output_names) end end)
                                |> merge_results()
                                |> map_success(fn new_list_props -> Map.put(props, list_prop_name, new_list_props) end)
      _else                  -> cur_prop_value = props[cur_prop_name]
                                apply_in(cur_prop_value, remaining_names, last_prop_path ++ [cur_prop_name], func, input_names, output_names)
                                |> map_success(fn new_props -> Map.put(props, cur_prop_name, new_props) end)
    end
  end

  @spec to_result(__MODULE__.t(), any()) :: {:error, any()} | {:ok, any()}
  def to_result(%__MODULE__{props: props, halt: halt}, output_names \\ []) do
    case halt do
      [error: error] -> {:error, {error, props}}
      _else          -> {:ok, build_result(props, output_names)}
    end
  end

  @spec build_result(map, [atom | binary]) :: map
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

  defp get_prop_path(options) do
    prop_path   = Keyword.get(options, :with, [])
    if is_list(prop_path), do: prop_path, else: [prop_path]
  end

  defp get_output_names(options) do
    output_names = Keyword.get(options, :outputs, Keyword.get(options, :output, []))
    if is_list(output_names), do: output_names, else: [output_names]
  end

  @spec put_prop(__MODULE__.t, atom | binary, any) :: __MODULE__.t
  defp put_prop(%__MODULE__{props: props} = pipeline, prop_name, value) do %{pipeline | props: props |> Map.put(prop_name, value)} end

  @spec put_error(__MODULE__.t, any) :: __MODULE__.t
  defp put_error(%__MODULE__{} = pipeline, error) do %{pipeline | halt: [error: error]} end

  @spec collect_error(__MODULE__.t, any) :: __MODULE__.t
  defp collect_error(%__MODULE__{collected_errors: collected_errors} = pipeline, error) do
    errors = if is_list(error), do: error, else: [error]
    %{pipeline | collected_errors: collected_errors ++ errors}
  end

  @spec stop_pipeline(__MODULE__.t) :: __MODULE__.t
  defp stop_pipeline(%__MODULE__{} = pipeline) do %{pipeline | halt: :short_circuit} end

  @spec map_success(result_type, (any -> result_type)) :: result_type
  defp map_success(result, func) do
    parse_result = fn result ->
        case result do
          :ok             -> :ok
          {:ok,    value} -> {:ok, value}
          {:error, error} -> {:error, error}
          value           -> {:ok, value}
        end
    end
    case result do
      :ok             -> func.(nil) |> parse_result.()
      {:ok,    value} -> func.(value) |> parse_result.()
      {:error, error} -> {:error, error}
    end
  end

  defp merge_results(funcs) do
    %{errors: errors, results: results} =
      funcs
      |> Enum.reduce(
           %{errors: [], results: []},
           fn func, %{errors: errors, results: results} ->
             case func.() do
               {:ok,   result} -> %{errors: errors, results: results ++ [result]}
               {:error, error} -> %{errors: errors ++ [error], results: results}
             end
           end)

    case errors do
      []    -> {:ok,   results}
      _else -> {:error, errors}
    end
  end

  defp log_step_info(
         %__MODULE__{
           log_level:  log_level,
           logger:    logger,
           steps_info: %{count: count} = steps_info,
           id:         pipeline_id,
           name:       pipeline_name} = pipeline,
         name, inputs, outputs, result) do

    count = count + 1
    name  = name || "step"
    name  = "#{name}:#{count}" <>
            ":( #{inputs |> Enum.join(",")} -> #{outputs |> Enum.join(",")} )" <>
            ":#{pipeline_name}(#{pipeline_id})"

    success_logger = case log_level do
      :debug -> logger[:debug]
      :info  -> logger[:info]
      :none  -> fn _ -> {} end
    end
    error_logger = case log_level do
      :none  -> fn _ -> {} end
      _      -> logger[:error]
    end

    log_success = fn-> success_logger.("#{name}:success") end
    log_failure = fn error -> error_logger.("#{name}:failure:#{inspect(error)}") end

    case result do
      {:ok,    _} -> log_success.()
      :ok         -> log_success.()
      {:error, e} -> log_failure.(e)
      :error      -> log_failure.(nil)
    end
    %{pipeline | steps_info: %{steps_info | count: count}}
  end
end
