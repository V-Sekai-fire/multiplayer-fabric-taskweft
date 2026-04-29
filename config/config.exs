import Config

config :taskweft, ecto_repos: [Taskweft.Repo]

import_config "#{config_env()}.exs"
