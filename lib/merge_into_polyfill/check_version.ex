defmodule MergeIntoPolyfill.CheckVersion do
  @pg15 Version.parse!("15.0.0")

  def child_spec(repo) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :check, [repo]}
    }
  end

  def check(repo) do
    with {:ok, version} <- get_postgres_version(repo) do
      builder = get_builder(version)
      Application.put_env(:merge_into_polyfill, __MODULE__, builder: builder)
      :ignore
    else
      error -> {:error, error}
    end
  end

  def get_postgres_version(repo) do
    with {:ok, %{rows: [[version]]}} <- repo.query("SHOW server_version;") do
      version
      |> maybe_add_patch()
      |> Version.parse()
    end
  end

  def get_builder(version) do
    case Version.compare(version, @pg15) do
      f when f in [:gt, :eq] -> MergeIntoPolyfill.Builders.MergeInto
      :lt -> MergeIntoPolyfill.Builders.Polyfill
    end
  end

  def maybe_add_patch(vs) do
    if Regex.match?(~r/^[0-9]+\.[0-9]+$/, vs) do
      vs <> ".0"
    else
      vs
    end
  end
end
