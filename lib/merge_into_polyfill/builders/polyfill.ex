defmodule MergeIntoPolyfill.Builders.Polyfill do
  @behaviour MergeIntoPolyfill.Builder

  def build_plan(target_schema, on_clause, data_source, when_clauses, opts) do
    Ecto.Multi.new()
  end
end
