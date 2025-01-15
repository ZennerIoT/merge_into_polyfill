defmodule MergeIntoPolyfill.Builders.Polyfill do
  @behaviour MergeIntoPolyfill.Builder
  import Ecto.Query
  import MergeIntoPolyfill.Builder

  def build_plan(target_schema, on_clause, data_source, when_clauses, opts) do
    prefix = Keyword.get(opts, :prefix, "public")

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
          prefix: ^prefix,
          right_join: s in ^make_source(data_source),
          as: :source,
          on: ^on_clause,
          left_join: class in "pg_class",
          on: class.oid == t.tableoid,
          as: :pg_class,
          left_join: namespace in "pg_namespace",
          on: class.relnamespace == namespace.oid,
          as: :pg_namespace,
          select:
            ^%{
              clauses: json_object,
              target_ctid: dynamic([target: t], fragment("?.ctid", t)),
              target_tablename: dynamic([pg_class: c], c.relname),
              target_schemaname: dynamic([pg_namespace: n], n.nspname),
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
          execute_action(candidates, action, repo, on_clause, target_schema, data_source, opts)
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

  @spec execute_action([any()], any, module, Ecto.Query.dynamic_expr(), module(), any(), keyword()) ::
          non_neg_integer()
  def execute_action(candidates, action, repo, on_clause, target_schema, data_source, opts)

  def execute_action([], _, _, _, _, _, _opts) do
    0
  end

  def execute_action(_, :nothing, _, _, _, _, _opts) do
    0
  end

  def execute_action(candidates, {:insert, fields}, repo, _on_clause, target_schema, data_source, opts) do
    candidates = Enum.map(candidates, & &1.source_id)

    query =
      from(ds in make_source(data_source),
        where: ds.id in type(^candidates, ^{:array, candidate_type(candidates)}),
        select: map(ds, ^fields)
      )

    repo.insert_all(target_schema, query, opts)
    |> elem(0)
  end

  def execute_action(candidates, :delete, repo, _on_clause, _target_schema, _, _opts) do
    candidates =
      Enum.group_by(candidates, & {&1.target_schemaname, &1.target_tablename}, & &1.target_ctid)

    for {{prefix, table}, ctids} <- candidates, length(ctids) > 0 do
      repo.delete_all(from(t in table, where: fragment("?.ctid", t) in ^ctids), prefix: prefix)
      |> elem(0)
    end
    |> Enum.sum()
  end

  def execute_action(candidates, {:update, updates}, repo, on_clause, _target_schema, data_source, _opts) do
    candidates =
      Enum.group_by(candidates, & {&1.target_schemaname, &1.target_tablename})

    for {{prefix, table}, candidates} <- candidates do
      ctids = Enum.map(candidates, & &1.target_ctid)

      query =
        from(t in table,
          as: :target,
          where: fragment("?.ctid", t) in ^ctids,
          join: ds in ^make_source(data_source),
          as: :source,
          on: ^on_clause,
          update: [set: ^updates]
        )

      repo.update_all(query, [], prefix: prefix)
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
