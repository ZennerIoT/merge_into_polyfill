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
      |> preprocess_version()
      |> Version.parse()
    end
  end

  def get_builder(version) do
    case Version.compare(version, @pg15) do
      f when f in [:gt, :eq] -> MergeIntoPolyfill.Builders.MergeInto
      :lt -> MergeIntoPolyfill.Builders.Polyfill
    end
  end

  @doc """
  Returns a cleaned up version string with 3 segments

      iex> MergeIntoPolyfill.CheckVersion.preprocess_version("3.5.0")
      "3.5.0"

      iex> MergeIntoPolyfill.CheckVersion.preprocess_version("3.5")
      "3.5.0"

      iex> MergeIntoPolyfill.CheckVersion.preprocess_version(" 13.6 (Debian 13.6-1.pgdg110+1)")
      "13.6.0"
  """
  def preprocess_version(vs) do
    [vs | _os_version] =
      vs
      |> String.trim()
      |> String.split(" ")

    if Regex.match?(~r/^[0-9]+\.[0-9]+$/, vs) do
      vs <> ".0"
    else
      vs
    end
  end
end
