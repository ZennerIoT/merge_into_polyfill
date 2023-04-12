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

The following is code from the test:

```elixir
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

merge_into(Book, as(:target).title == as(:source).title, source_query) do
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
```

For Postgres 15 and newer, it will run this query:

```sql
MERGE INTO "books" AS b0 
USING (SELECT 
  sf0 + 0 AS "id", 
  concat($1::text, sf0) AS "title", 
  sf0 + 2000 AS "year" 
  FROM generate_series(1, 10) AS sf0
) AS s1 
ON b0."title" = s1."title" 
WHEN MATCHED AND s1."year" >= 2008 
  THEN UPDATE SET "year" = s1."year" 
WHEN MATCHED AND b0."title" = $2 
  THEN DELETE 
WHEN MATCHED AND TRUE 
  THEN UPDATE SET "title" = concat(b0."title", ' (', s1."year", ')') 
WHEN NOT MATCHED AND TRUE 
  THEN INSERT ("title", "year") VALUES (s1."title", s1."year");
-- ["Book ", "Book 2"]
```

For anything older than Postgres 15, these queries will be executed:

```sql
begin;

SELECT jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_build_object('target_id', b0."id", 'source_id', s1."id")::jsonb, $1::text[], to_jsonb(NOT (b0."id" IS NULL) AND (s1."year" >= 2008)::boolean))::jsonb, $2::text[], to_jsonb(NOT (b0."id" IS NULL) AND (b0."title" = $3)::boolean))::jsonb, $4::text[], to_jsonb(NOT (b0."id" IS NULL) AND TRUE::boolean))::jsonb, $5::text[], to_jsonb((b0."id" IS NULL) AND TRUE::boolean)) FROM "books" AS b0 RIGHT OUTER JOIN (SELECT sf0 + 0 AS "id", concat($6::text, sf0) AS "title", sf0 + 2000 AS "year" FROM generate_series(1, 10) AS sf0) AS s1 ON b0."title" = s1."title";
-- [["0"], ["1"], "Book 2", ["2"], ["3"], "Book "]

UPDATE "books" AS b0 SET "year" = s1."year" FROM (SELECT sf0 + 0 AS "id", concat($1::text, sf0) AS "title", sf0 + 2000 AS "year" FROM generate_series(1, 10) AS sf0) AS s1 WHERE (b0."title" = s1."title") AND (b0."id" = ANY($2));
-- ["Book ", [455]]

DELETE FROM "books" AS b0 WHERE (b0."id" = ANY($1));
-- [[454]]

UPDATE "books" AS b0 SET "title" = concat(b0."title", ' (', s1."year", ')') FROM (SELECT sf0 + 0 AS "id", concat($1::text, sf0) AS "title", sf0 + 2000 AS "year" FROM generate_series(1, 10) AS sf0) AS s1 WHERE (b0."title" = s1."title") AND (b0."id" = ANY($2));
-- ["Book ", [456]]

INSERT INTO "books" ("title","year") (SELECT s0."title", s0."year" FROM (SELECT sf0 + 0 AS "id", concat($1::text, sf0) AS "title", sf0 + 2000 AS "year" FROM generate_series(1, 10) AS sf0) AS s0 WHERE (s0."id" = ANY($2)));
-- ["Book ", [5, 7, 4, 9, 8, 6, 1]]

commit;
```