defmodule MergeIntoPolyfillTest do
  use MergeIntoPolyfill.DataCase
  doctest MergeIntoPolyfill
  import MergeIntoPolyfill
  import Ecto.Query

  test "compiles the merge_into macro without compile errors" do
    assert :ignore = MergeIntoPolyfill.CheckVersion.check(Repo)
    values = values(Book, [:title, :year], [%{title: "Book 1", year: 2008}])

    plan =
      merge_into(Book, as(:target).title == as(:source).title, values) do
        matched?() and as(:target).title == ^"Book 2" -> delete()
        matched?() -> update([:year])
        not matched?() and as(:source).year > 2001 -> insert([:year])
        not matched?() -> insert([:title, :year])
      end

    assert %Ecto.Multi{} = plan
  end

  test "raises when no builder has been set" do
    assert_raise RuntimeError, fn ->
      values = values(Book, [:title, :year], [%{title: "Book 1", year: 2008}])

      merge_into(Book, true, values, builder: nil) do
        matched?() -> delete()
      end
    end
  end

  test "source query polyfill" do
    test_source_query(MergeIntoPolyfill.Builders.Polyfill)
  end

  test "source query merge into" do
    test_source_query(MergeIntoPolyfill.Builders.MergeInto)
  end

  test "values list polyfill" do
    test_values_list(MergeIntoPolyfill.Builders.Polyfill)
  end

  test "values list merge into" do
    test_values_list(MergeIntoPolyfill.Builders.MergeInto)
  end

  def test_source_query(builder) do
    Repo.insert(%Book{title: "Book 2", year: 1999})
    Repo.insert(%Book{title: "Book 10", year: 2007})
    Repo.insert(%Book{title: "Book 3", year: 2000})

    source_query =
      from(gs in fragment("generate_series(1, 10)"),
        select: %{
          id: gs + 0,
          title: fragment("concat(?::text, ?)", ^"Book ", gs),
          year: gs + 2000
        }
      )

    merge_into(Book, as(:target).title == as(:source).title, source_query, builder: builder) do
      matched?() and as(:source).year >= 2008 ->
        update([:year])

      matched?() and as(:target).title == ^"Book 2" ->
        delete()

      matched?() ->
        update(title: fragment("concat(?, ' (', ?, ')')", as(:target).title, as(:source).year))

      not matched?() ->
        insert([:title, :year])
    end
    |> Repo.transaction()

    assert Repo.aggregate(Book, :count, :id) == 9

    # test all match cases
    assert Repo.one(from(b in Book, where: b.title == ^"Book 10", select: b.year)) == 2010
    assert is_nil(Repo.get_by(Book, title: "Book 2"))
    assert not is_nil(Repo.get_by(Book, title: "Book 3 (2003)"))
    assert %{year: 2006} = Repo.get_by(Book, title: "Book 6")
  end

  def test_values_list(builder) do
    Repo.insert(%Book{title: "Book 2", year: 1999})
    Repo.insert(%Book{title: "Book 3", year: 2000})
    Repo.insert(%Book{title: "Book 10", year: 2007})

    values = MergeIntoPolyfill.values(Book, [:id, :title, :year], [
      %{id: 1, title: "Abc", year: 1999},
      %{id: 2, title: "Def", year: 2000},
      %{id: 3, title: "Bubatz", year: 2023}
    ])

    merge_into(Book, as(:target).year == as(:source).year, values, builder: builder) do
      matched?() ->
        update([:title])

      not matched?() ->
        insert([:title, :year])
    end
    |> Repo.transaction()

    assert Repo.aggregate(Book, :count, :id) == 4

    # test all match cases
    assert Repo.get_by(Book, year: 1999).title == "Abc"
    assert Repo.get_by(Book, year: 2000).title == "Def"
    assert not is_nil(Repo.get_by(Book, year: 2023))
  end

  test "get_builder" do
    import MergeIntoPolyfill.CheckVersion
    alias MergeIntoPolyfill.Builders.{Polyfill, MergeInto}
    assert get_builder(Version.parse!("15.5.1")) == MergeInto
    assert get_builder(Version.parse!("23.5.1")) == MergeInto
    assert get_builder(Version.parse!("14.3.9")) == Polyfill
    assert get_builder(Version.parse!("12.0.0")) == Polyfill
  end
end
