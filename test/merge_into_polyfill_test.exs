defmodule MergeIntoPolyfillTest do
  use ExUnit.Case
  doctest MergeIntoPolyfill
  import MergeIntoPolyfill
  import Ecto.Query

  test "greets the world" do
    values = values(Book, [%{title: "Book 1", year: 2008}])
    plan =
      merge_into(Book, as(:target).title == as(:source).title, values) do
        matched?() and as(:target).title == ^"Book 2" -> :delete
        matched?() -> {:update, [:year]}
        not matched?() -> :insert
      end

  """
  MERGE INTO books AS target
  USING VALUES(('Book 1', 2008)) AS source(title text, year integer)
  ON source.title = target.title
  MATCH
  WHEN MATCHED AND target.title = 'Book 2' THEN DELETE
  WHEN MATCHED THEN UPDATE SET year = source.year
  WHEN NOT MATCHED THEN INSERT (title, year) VALUES (source.title, source.year);
  """
  end
end
