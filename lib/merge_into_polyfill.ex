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
      default_builder =
        Application.get_env(:merge_into_polyfill, MergeIntoPolyfill.CheckVersion, [])
        |> Keyword.get(:builder)

      builder = Keyword.get(unquote(opts), :builder, default_builder)

      if is_nil(builder) do
        raise RuntimeError, """
        A builder for the merge query must be set in the opts, or by adding

          {MergeIntoPolyfill.CheckVersion, MyRepo}

        to your application spec directly after the repo has been started.
        Possible builders:

         * MergeIntoPolyfill.Builders.MergeInto for PostgreSQL >= 15
         * MergeIntoPolyfill.Builders.Polyfill for anything older than PostgreSQL 15
        """
      end

      builder.build_plan(
        unquote_splicing([target_schema, on_clause, data_source, when_clauses, opts])
      )
    end
  end

  defmacro values(schema, fields, values) do
    import Ecto.Query

    selects =
      Enum.map(fields, fn field ->
        expr =
          quote do
            fragment(
              "unnest(?)",
              type(
                ^Enum.map(unquote(values), &Map.get(&1, unquote(field))),
                ^{:array, unquote(schema).__schema__(:type, unquote(field))}
              )
            )
          end

        {field, expr}
      end)

    quote do
      import Ecto.Query
      from(q in fragment("generate_series(0, 0)"), select: unquote({:%{}, [], selects}))
    end
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
      case {action, match} do
        {{:insert, _, [fields]}, :not_matched} ->
          {:insert, fields}

        {{:delete, _, []}, :matched} ->
          :delete

        {{:do_nothing, _, []}, _} ->
          :nothing

        {{:update, _, [updates]}, :matched} ->
          build_update(updates)

        {{:insert, ctx, _}, :matched} ->
          raise CompileError,
            line: Keyword.get(ctx, :line),
            description: "insert/1 can only be used in combination with `not matched?`"

        {{:update, ctx, _}, :not_matched} ->
          raise CompileError,
            line: Keyword.get(ctx, :line),
            description: "update/1 can only be used in combination with `not matched?`"
      end

    {:{}, [], [match, condition, action]}
  end

  @spec build_update([atom()]) :: {:update, [{atom(), Ecto.Query.dynamic_expr()}]}
  defp build_update(updates) do
    updates =
      Enum.map(updates, fn
        field when is_atom(field) ->
          expr = wrap_dynamic({{:., [], [{:as, [], [:source]}, field]}, [no_parens: true], []})
          {field, expr}

        {field, expr} ->
          expr = wrap_dynamic(expr)
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

  def insert(fields) do
    _ = fields

    raise RuntimeError,
      message: "not meant to be called directly, use in merge_into polyfill macro."
  end

  def update(updates) do
    _ = updates

    raise RuntimeError,
      message: "not meant to be called directly, use in merge_into polyfill macro."
  end

  def delete() do
    raise RuntimeError,
      message: "not meant to be called directly, use in merge_into polyfill macro."
  end

  def do_nothing() do
    raise RuntimeError,
      message: "not meant to be called directly, use in merge_into polyfill macro."
  end
end
