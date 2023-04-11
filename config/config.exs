import Config

if Mix.env() == :test do
  config :merge_into_polyfill, ecto_repos: [Repo]
end
