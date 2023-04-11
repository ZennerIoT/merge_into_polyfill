defmodule MergeIntoPolyfillTest do
  use ExUnit.Case
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
end
