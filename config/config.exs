import Config

config :instructor,
  adapter: Instructor.Adapters.OpenAI,
  openai: [
    api_key: "local",
    api_url: "http://127.0.0.1:9000"
  ]
