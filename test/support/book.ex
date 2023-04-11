defmodule Book do
  use Ecto.Schema

  schema "books" do
    field(:title, :string)
    field(:year, :integer)
  end
end
