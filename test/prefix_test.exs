defmodule PrefixTest do
  use MergeIntoPolyfill.DataCase
  import MergeIntoPolyfill
  import Ecto.Query

  test "prefixes work with polyfill" do
    prefix = "test_with_polyfill"
    setup_table(prefix)
    MergeIntoPolyfillTest.test_source_query(MergeIntoPolyfill.Builders.Polyfill, prefix: prefix)
  end

  test "prefixes work with merge into" do
    prefix = "test_with_merge_into"
    setup_table(prefix)
    MergeIntoPolyfillTest.test_source_query(MergeIntoPolyfill.Builders.MergeInto, prefix: prefix)
  end

  def setup_table(prefix) do
    Repo.query!("CREATE SCHEMA IF NOT EXISTS #{prefix}")

    Repo.query!("""
    CREATE TABLE "#{prefix}"."books" (
      "id" BIGSERIAL PRIMARY KEY,
      "title" text,
      "year" integer
    )
    """)
  end
end
