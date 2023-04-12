defmodule MergeIntoPolyfillTest do
  use MergeIntoPolyfill.DataCase
  doctest MergeIntoPolyfill
  import MergeIntoPolyfill
  import Ecto.Query

  test "compiles the merge_into macro without compile errors" do
    values = values(Book, [%{title: "Book 1", year: 2008}])

    plan =
      merge_into(Book, as(:target).title == as(:source).title, values) do
        matched?() and as(:target).title == ^"Book 2" -> delete()
        matched?() -> update([:year])
        not matched?() and as(:source).year > 2001 -> update([:year])
        not matched?() -> insert([:title, :year])
      end

    assert %Ecto.Multi{} = plan
  end

  test "polyfill test" do
    test_poly_1(MergeIntoPolyfill.Builders.Polyfill)
  end

  test "merge into test" do
    test_poly_1(MergeIntoPolyfill.Builders.MergeInto)
  end

  def test_poly_1(builder) do
    Repo.insert(%Book{title: "Book 2", year: 1999})
    Repo.insert(%Book{title: "Book 10", year: 2007})
    Repo.insert(%Book{title: "Book 3", year: 2000})

    source_query = from gs in fragment("generate_series(1, 10)"),
      select: %{
        id: gs + 0,
        title: fragment("concat(?::text, ?)", ^"Book ", gs),
        year: gs + 2000
      }

    merge_into(Book, as(:target).title == as(:source).title, source_query, builder: builder) do
      matched?() and as(:source).year >= 2008 -> update([:year])
      matched?() and as(:target).title == ^"Book 2" -> delete()
      matched?() -> update(title: fragment("concat(?, ' (', ?, ')')", as(:target).title, as(:source).year))
      not matched?() -> insert([:title, :year])
    end
    |> Repo.transaction()

    assert Repo.aggregate(Book, :count, :id) == 9

    # test all match cases
    assert Repo.one(from(b in Book, where: b.title == ^"Book 10", select: b.year)) == 2010
    assert is_nil(Repo.get_by(Book, title: "Book 2"))
    assert not is_nil(Repo.get_by(Book, title: "Book 3 (2003)"))
    assert %{year: 2006} = Repo.get_by(Book, title: "Book 6")
  end
end
