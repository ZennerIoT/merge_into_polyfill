defmodule MergeIntoPolyfill.Builders.MergeInto do
  @behaviour MergeIntoPolyfill.Builder
  import Ecto.Query

  def build_plan(target_schema, on_clause, data_source, when_clauses, _opts) do
    Ecto.Multi.new()
    |> Ecto.Multi.run(:merge_into, fn repo, _context ->
      whens =
        Enum.map(when_clauses, fn
          {:matched, condition, action} ->
            dynamic(fragment("WHEN MATCHED AND ? THEN ?", ^condition, ^action_to_dynamic(action)))

          {:not_matched, condition, action} ->
            dynamic(
              fragment("WHEN NOT MATCHED AND ? THEN ?", ^condition, ^action_to_dynamic(action))
            )
        end)
        |> Enum.reduce(fn a, b ->
          dynamic(fragment("? ?", ^b, ^a))
        end)

      query =
        from(t in target_schema,
          as: :target,
          right_join: ds in ^make_source(data_source),
          as: :source,
          on: ^on_clause,
          select: nil,
          where: ^whens
        )

      {sql, params} = repo.to_sql(:all, query)
      sql =
        sql
        |> String.replace("SELECT TRUE FROM", "MERGE INTO")
        |> String.replace("RIGHT OUTER JOIN", "USING")
        |> String.replace("WHERE (", "")
        |> String.replace(~r/\)$/, "")

      repo.query!(sql, params)
      {:ok, nil}
    end)
  end

  def make_source(%Ecto.Query{} = query) do
    subquery(query)
  end

  def action_to_dynamic(:delete) do
    dynamic(fragment("DELETE"))
  end

  def action_to_dynamic(:nothing) do
    dynamic(fragment("DO NOTHING"))
  end

  def action_to_dynamic({:insert, fields}) do
    field_list =
      Enum.map(fields, fn field ->
        dynamic(fragment("?", literal(^to_string(field))))
      end)
      |> join_list()

    values_list =
      Enum.map(fields, fn field ->
        dynamic(field(as(:source), ^field))
      end)
      |> join_list()

    dynamic(fragment("INSERT (?) VALUES (?)", ^field_list, ^values_list))
  end

  def action_to_dynamic({:update, updates}) do
    updates = Enum.map(updates, fn {field, expr} ->
      dynamic(fragment("? = ?", literal(^to_string(field)), ^expr))
    end)
    |> join_list()

    dynamic(fragment("UPDATE SET ?", ^updates))
  end

  defp join_list(list) do
    Enum.reduce(list, fn a, b ->
      dynamic(fragment("?, ?", ^b, ^a))
    end)
  end
end
