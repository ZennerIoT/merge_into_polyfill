defmodule MergeIntoPolyfillTest do
  use MergeIntoPolyfill.DataCase
  doctest MergeIntoPolyfill
  import MergeIntoPolyfill
  import Ecto.Query

  test "compiles the merge_into macro without compile errors" do
    values = values(Book, [%{title: "Book 1", year: 2008}])

    plan =
      merge_into(Book, as(:target).title == as(:source).title, values) do
        matched?() and as(:target).title == ^"Book 2" -> :delete
        matched?() -> {:update, [:year]}
        not matched?() and as(:source).year > 2001 -> {:update, [:year]}
        not matched?() -> :insert
      end

    assert %Ecto.Multi{} = plan
  end

  test "polyfill test" do
    Repo.insert(%Book{title: "Book 2", year: 1999})
    Repo.insert(%Book{title: "Book 10", year: 2007})
    Repo.insert(%Book{title: "Book 3", year: 2000})

    source_query = from gs in fragment("generate_series(1, 10)"),
      select: %{
        id: gs + 0,
        title: fragment("concat(?::text, ?)", ^"Book ", gs),
        year: gs + 2000
      }

    merge_into(Book, as(:target).title == as(:source).title, source_query, builder: MergeIntoPolyfill.Builders.Polyfill) do
      matched?() and as(:source).year >= 2008 -> {:update, [:year]}
      matched?() and as(:target).title == ^"Book 2" -> :delete
      matched?() -> {:update, [:year]}
      not matched?() -> :insert
    end
    |> Repo.transaction()
  end
end
