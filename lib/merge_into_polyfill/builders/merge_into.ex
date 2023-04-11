defmodule MergeIntoPolyfill.Builders.MergeInto do
  @behaviour MergeIntoPolyfill.Builder

  def build_plan(target_schema, on_clause, data_source, when_clauses, opts) do
    Ecto.Multi.new()
    |> Ecto.Multi.run(:merge_into, fn repo, _context ->
      nil
      # TODO generate SQL
      # TODO dump params and make nice parameter list
      # call repo.query!(sql, parameters)
      # done!
    end)
  end
end
