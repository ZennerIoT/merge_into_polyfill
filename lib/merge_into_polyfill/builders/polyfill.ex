defmodule MergeIntoPolyfill.Builders.Polyfill do
  @behaviour MergeIntoPolyfill.Builder
  import Ecto.Query

  def build_plan(target_schema, on_clause, data_source, when_clauses, _opts) do
    Ecto.Multi.new()
    |> Ecto.Multi.run(:find_matches, fn repo, _context ->

      json_object = dynamic(fragment("jsonb_build_object('target_id', ?, 'source_id', ?)", as(:target).id, as(:source).id))

      json_object =
        Enum.map(when_clauses, fn
          {:matched, condition, _action} ->
            dynamic(not is_nil(as(:target).id) and ^condition)

          {:not_matched, condition, _action} ->
            dynamic(is_nil(as(:target).id) and ^condition)
        end)
        |> Enum.with_index()
        |> Enum.reduce(json_object, fn {match, index}, acc ->
          dynamic(fragment("jsonb_set(?::jsonb, ?::text[], to_jsonb(?::boolean))", ^acc, ^[to_string(index)], ^match))
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
      |> Enum.each(fn {{_, _, action}, index}) do
        candidates = find_candidates(result, index)
      end
      {:ok, nil}
    end)
  end

  def find_candidates(result, 0) do
    Enum.filter(result, &Map.get(&1, "0"))
  end
  def find_candidates(result, index) do
    Enum.reject()
  end

  def make_source(%Ecto.Query{} = query) do
    subquery(query)
  end
end
