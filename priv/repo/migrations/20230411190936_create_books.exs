defmodule Repo.Migrations.CreateBooks do
  use Ecto.Migration

  def change do
    create table(:books) do
      add :title, :text
      add :year, :integer
    end
  end
end
