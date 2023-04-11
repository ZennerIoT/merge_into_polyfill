defmodule Repo do
  use Ecto.Repo,
    otp_app: :merge_into_polyfill,
    adapter: Ecto.Adapters.Postgres

  def init(_context, config) do
    {:ok, Keyword.merge(config, [
      database: "merge_into_polyfill_test",
      username: "postgres",
      password: "postgres",
      hostname: "localhost",
      pool_workers: 5
    ])}
  end
end
