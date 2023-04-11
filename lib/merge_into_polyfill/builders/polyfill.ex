defmodule MergeIntoPolyfill.Builders.Polyfill do
  @behaviour MergeIntoPolyfill.Builder
  import Ecto.Query

  def build_plan(target_schema, on_clause, data_source, when_clauses, opts) do
    Ecto.Multi.new()
    |> Ecto.Multi.run(:find_matches, fn repo, _context ->
      base_query =
        from(t in target_schema,
          as: :target,
          full_join: s in ^make_source(data_source),
          as: :source,
          on: ^on_clause
        )

      query =
        Enum.reduce(when_clauses, fn
          # TODO merge_select one column for each when clause, should return a boolean whether this when clause should be
          # executed
          _ -> :ok
        end)

      repo.all(query)
    end)
    |> Ecto.Multi.run(:fulfil_match_clauses, fn repo, %{find_matches: result} ->
      nil
      # TODO
    end)
  end

  def make_source(%Ecto.Query{} = query) do
    query
  end
end
