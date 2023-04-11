defmodule MergeIntoPolyfill do
  defmacro merge_into(target_schema, on_clause, data_source, opts \\ [], when_clauses) do
    on_clause =
      on_clause
      |> wrap_dynamic()

    when_clauses =
      when_clauses
      |> Keyword.fetch!(:do)
      |> Enum.map(&compile_when_clause/1)

    quote do
      builder = Keyword.get(unquote(opts), :builder, MergeIntoPolyfill.Builders.MergeInto)

      builder.build_plan(
        unquote_splicing([target_schema, on_clause, data_source, when_clauses, opts])
      )
    end
  end

  @spec values(module(), [struct()]) :: any
  def values(schema, values) do
  end

  defp wrap_dynamic(quoted) do
    {:dynamic, [context: Elixir, imports: [{1, Ecto.Query}, {2, Ecto.Query}]], [quoted]}
  end

  @spec compile_when_clause(tuple()) :: Macro.t()
  defp compile_when_clause({:->, _, [[condition], action]}) do
    {match, condition} =
      case condition do
        {:and, _ctx, [match?, condition]} -> {compile_matched(match?), wrap_dynamic(condition)}
        matched? -> {compile_matched(matched?), wrap_dynamic(true)}
      end

    action =
      case action do
        :insert ->
          :insert

        :delete ->
          :delete

        :nothing ->
          :nothing

        {:update, [{field, _} | _] = updates} when is_atom(field) ->
          updates =
            Enum.map(updates, fn {field, expr} ->
              {field, expr |> wrap_dynamic()}
            end)

          {:update, updates}

        {:update, [field | _] = fields} when is_atom(field) ->
          build_update(fields)
      end

    {:{}, [], [match, condition, action]}
  end

  @spec build_update([atom()]) :: {:update, [{atom(), Ecto.Query.dynamic_expr()}]}
  defp build_update(fields) do
    updates =
      Enum.map(fields, fn field ->
        expr = wrap_dynamic({{:., [], [{:as, [], [:source]}, field]}, [no_parens: true], []})
        {field, expr}
      end)

    {:update, updates}
  end

  defp compile_matched({:matched?, _, []}), do: :matched
  defp compile_matched({:not, _, [{:matched?, _, []}]}), do: :not_matched

  def matched?() do
    raise RuntimeError,
      message: "not meant to be called directly, use in merge_into polyfill macro."
  end
end
