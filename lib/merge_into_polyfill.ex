defmodule MergeIntoPolyfill do
  defmacro merge_into(target_schema, on_clause, data_source, when_clauses) do
    IO.inspect(target_schema, label: "merge into")

    on_clause
    |> wrap_dynamic()
    |> quoted_to_query(__CALLER__)
    |> IO.inspect(label: "on")

    when_clauses = Keyword.fetch!(when_clauses, :do)
    Enum.each(when_clauses, fn {:->, _, [condition, action]} ->
      case action do
        {:update, updates} ->
          Enum.each(updates, fn {key, value} ->
            value
            |> wrap_dynamic()
            |> quoted_to_query(__CALLER__)
            |> IO.inspect(label: "update(#{inspect key})")
          end)
        :insert -> IO.puts("insert")
        :delete -> IO.puts("delete")
      end
    end)
  end

  @spec values(module(), [struct()]) :: any
  def values(schema, values) do

  end

  defp wrap_dynamic(quoted) do
    {:dynamic, [], [quoted]}
  end

  defp quoted_to_query(query, env) do
    {query, _} = Code.eval_quoted(query, [], env)
    query
  end
end
