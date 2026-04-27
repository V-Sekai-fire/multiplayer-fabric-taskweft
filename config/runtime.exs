import Config
import Dotenvy

source([".env", System.get_env()])

if api_key = env("OPENROUTER_API_KEY", :string, nil) do
  config :instructor,
    openai: [
      api_key: api_key,
      api_url: "https://openrouter.ai/api"
    ]
end
