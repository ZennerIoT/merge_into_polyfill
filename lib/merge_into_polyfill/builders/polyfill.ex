defmodule MergeIntoPolyfill.Builders.Polyfill do
  @behaviour MergeIntoPolyfill.Builder
  import Ecto.Query
  import MergeIntoPolyfill.Builder

  def build_plan(target_schema, on_clause, data_source, when_clauses, _opts) do
    Ecto.Multi.new()
    |> Ecto.Multi.run(:find_matches, fn repo, _context ->
      json_object =
        dynamic(type(^%{}, :map))

      json_object =
        Enum.map(when_clauses, fn
          {:matched, condition, _action} ->
            dynamic(not is_nil(as(:target).id) and ^condition)

          {:not_matched, condition, _action} ->
            dynamic(is_nil(as(:target).id) and ^condition)
        end)
        |> Enum.with_index()
        |> Enum.reduce(json_object, fn {match, index}, acc ->
          dynamic(
            fragment(
              "jsonb_set(?::jsonb, ?::text[], to_jsonb(?::boolean))",
              ^acc,
              ^[to_string(index)],
              ^match
            )
          )
        end)

      query =
        from(t in target_schema,
          as: :target,
          right_join: s in ^make_source(data_source),
          as: :source,
          on: ^on_clause,
          left_join: class in "pg_class",
          on: class.oid == t.tableoid,
          as: :pg_class,
          select:
            ^%{
              clauses: json_object,
              target_ctid: dynamic([target: t], fragment("?.ctid", t)),
              target_tablename: dynamic([pg_class: c], c.relname),
              source_id: dynamic(as(:source).id)
            }
        )

      {:ok, repo.all(query)}
    end)
    |> Ecto.Multi.run(:merge, fn repo, %{find_matches: result} ->
      affected =
        when_clauses
        |> Enum.with_index()
        |> Enum.map(fn {{_, _, action}, index} ->
          candidates = find_candidates(result, index)
          execute_action(candidates, action, repo, on_clause, target_schema, data_source)
        end)
        |> Enum.sum()

      {:ok, affected}
    end)
  end

  def find_candidates(result, 0) do
    Enum.filter(result, &Map.get(&1.clauses, "0"))
  end

  def find_candidates(result, index) do
    Enum.reject(result, fn map ->
      Enum.any?(0..(index - 1), &Map.get(map.clauses, to_string(&1)))
    end)
    |> Enum.filter(&Map.get(&1.clauses, to_string(index)))
  end

  @spec execute_action([any()], any, module, Ecto.Query.dynamic_expr(), module(), any()) ::
          non_neg_integer()
  def execute_action(candidates, action, repo, on_clause, target_schema, data_source)

  def execute_action([], _, _, _, _, _) do
    0
  end

  def execute_action(_, :nothing, _, _, _, _) do
    0
  end

  def execute_action(candidates, {:insert, fields}, repo, _on_clause, target_schema, data_source) do
    candidates = Enum.map(candidates, & &1.source_id)

    query =
      from(ds in make_source(data_source),
        where: ds.id in type(^candidates, ^{:array, candidate_type(candidates)}),
        select: map(ds, ^fields)
      )

    repo.insert_all(target_schema, query)
    |> elem(0)
  end

  def execute_action(candidates, :delete, repo, _on_clause, _target_schema, _) do
    candidates =
      Enum.group_by(candidates, & &1.target_tablename, & &1.target_ctid)

    for {target_tablename, ctids} <- candidates, length(ctids) > 0 do
      repo.delete_all(from(t in target_tablename, where: fragment("?.ctid", t) in ^ctids))
      |> elem(0)
    end
    |> Enum.sum()
  end

  def execute_action(candidates, {:update, updates}, repo, on_clause, _target_schema, data_source) do
    candidates =
      Enum.group_by(candidates, & &1.target_tablename)

    for {target_tablename, candidates} <- candidates do
      ctids = Enum.map(candidates, & &1.target_ctid)

      query =
        from(t in target_tablename,
          as: :target,
          where: fragment("?.ctid", t) in ^ctids,
          join: ds in ^make_source(data_source),
          as: :source,
          on: ^on_clause,
          update: [set: ^updates]
        )

      repo.update_all(query, [])
      |> elem(0)
    end
    |> Enum.sum()
  end

  defp candidate_type([sample | _]) do
    cond do
      is_integer(sample) -> :integer
      Ecto.UUID.dump(sample) != :error -> Ecto.UUID
      is_binary(sample) -> :string
    end
  end
end
