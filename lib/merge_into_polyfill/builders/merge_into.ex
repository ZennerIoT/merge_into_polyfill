defmodule MergeIntoPolyfill.Builders.MergeInto do
  @behaviour MergeIntoPolyfill.Builder

  def build_plan(target_schema, on_clause, data_source, when_clauses, opts) do
    IO.inspect when_clauses
    Ecto.Multi.new()
    |> Ecto.Multi.run(:merge_into, fn repo, _context ->
      repo.query()
    end)
  end
end
