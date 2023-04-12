defmodule MergeIntoPolyfill.Builders.Polyfill do
  @behaviour MergeIntoPolyfill.Builder
  import Ecto.Query

  def build_plan(target_schema, on_clause, data_source, when_clauses, _opts) do
    Ecto.Multi.new()
    |> Ecto.Multi.run(:find_matches, fn repo, _context ->
      json_object =
        dynamic(
          fragment(
            "jsonb_build_object('target_id', ?, 'source_id', ?)",
            as(:target).id,
            as(:source).id
          )
        )

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
          select: ^json_object
        )

      {:ok, repo.all(query)}
    end)
    |> Ecto.Multi.run(:fulfil_match_clauses, fn repo, %{find_matches: result} ->
      when_clauses
      |> Enum.with_index()
      |> Enum.each(fn {{_, _, action}, index} ->
        candidates = find_candidates(result, index)
        execute_action(candidates, action, repo, on_clause, target_schema, data_source)
      end)

      {:ok, nil}
    end)
  end

  def find_candidates(result, 0) do
    Enum.filter(result, &Map.get(&1, "0"))
  end

  def find_candidates(result, index) do
    Enum.reject(result, fn map ->
      Enum.any?(0..(index - 1), &Map.get(map, to_string(&1)))
    end)
    |> Enum.filter(&Map.get(&1, to_string(index)))
  end

  def execute_action(candidates, action, repo, on_clause, target_schema, data_source)

  def execute_action([], _, _, _, _, _) do
    :ok
  end

  def execute_action(_, :nothing, _, _, _, _) do
    :ok
  end

  def execute_action(candidates, {:insert, fields}, repo, _on_clause, target_schema, data_source) do
    candidates = Enum.map(candidates, & &1["source_id"])

    query =
      from(ds in make_source(data_source),
        where: ds.id in ^candidates,
        select: map(ds, ^fields)
      )

    repo.insert_all(target_schema, query)
  end

  def execute_action(candidates, :delete, repo, _on_clause, target_schema, _) do
    candidates = Enum.map(candidates, & &1["target_id"])
    query = from(t in target_schema, where: t.id in ^candidates)
    repo.delete_all(query)
  end

  def execute_action(candidates, {:update, updates}, repo, on_clause, target_schema, data_source) do
    candidates = Enum.map(candidates, & &1["target_id"])
    query = from t in target_schema, as: :target,
      where: t.id in ^candidates,
      join: ds in ^make_source(data_source), as: :source,
      on: ^on_clause,
      update: [set: ^updates]

    repo.update_all(query, [])
  end

  def make_source(%Ecto.Query{} = query) do
    subquery(query)
  end
end
