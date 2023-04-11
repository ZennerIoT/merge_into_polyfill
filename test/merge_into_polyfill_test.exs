defmodule MergeIntoPolyfillTest do
  use ExUnit.Case
  doctest MergeIntoPolyfill
  import MergeIntoPolyfill
  import Ecto.Query

  test "greets the world" do
    values = values(Book, [%{title: "Book 1", year: 2008}])
    plan =
      merge_into(Book, as(:target).title == as(:source).title, values) do
        matched?() -> {:update, year: as(:source).year}
        matched?() and b.title == ^"Book 2" -> :delete
        not matched?() -> :insert
      end


  end
end
