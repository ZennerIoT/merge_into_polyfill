# MergeIntoPolyfill

Features: 
 
 * provide a DSL to express postgres [`MERGE` queries](https://www.postgresql.org/docs/current/sql-merge.html)
 * depending on the current postgres version, 
   * `>= 15` runs the query as a single `MERGE` query
   * `< 15` will run a more involved plan in a transaction:
     * select query to find the matches specified
     * one insert/update/delete query for each match
   * both ways have the same effect, but the `MERGE` query is more performant, 
     since the parameters have to be sent only once.

## Examples

```elixir
merge_into(Book, as(:target).title == as(:source).title, values) do
  matched?() and as(:target).title == ^"Book 2" -> delete()
  matched?() -> update([:year])
  not matched?() and as(:source).year > 2001 -> update([:year])
  not matched?() -> insert([:title, :year])
end
```