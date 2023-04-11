defmodule MergeIntoPolyfill.Builder do
  @type merge_action ::
    :insert |
    {:update, [{atom(), Ecto.Query.dynamic_expr()}]} |
    :delete

  @type when_clause :: {:matched | :not_matched, Ecto.Query.dynamic_expr(), merge_action()}
  @type data_source :: Ecto.Query.t() | MergeIntoPolyfill.Values.t()

  @callback build_plan(target_schema :: module(), on_clause :: Ecto.Query.dynamic_expr(), data_source, when_clauses :: [when_clause()], opts :: keyword()) :: Ecto.Multi.t()
end
