# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule Instructor.Adapters.TurboquantLlm do
  @moduledoc """
  Instructor adapter for TurboquantLlm (local llama.cpp NIF).

  Routes `Instructor.chat_completion/1` through a running `TurboquantLlm.Session`,
  injecting the JSON schema as a user-turn instruction so the model returns
  structured output that Instructor can validate against the Ecto schema.

  ## Configuration

      # config/runtime.exs
      config :instructor,
        adapter: Instructor.Adapters.TurboquantLlm,
        turboquant_llm: [session: :gepa_session]

  ## Supervision

  The session must be started before any inference call:

      children = [
        {TurboquantLlm.Session,
         model_path: "/models/my-model.gguf",
         n_gpu_layers: -1,
         name: :gepa_session}
      ]
  """

  @behaviour Instructor.Adapter

  @impl true
  def chat_completion(params, config) do
    session = session_name(config)
    messages = build_messages(params)

    case apply(TurboquantLlm, :chat, [session, messages]) do
      {:ok, text} ->
        case Jason.decode(String.trim(text)) do
          {:ok, map} ->
            {:ok, nil, map}

          {:error, _} ->
            {:error, "TurboquantLlm returned non-JSON: #{String.slice(text, 0, 200)}"}
        end

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  @impl true
  def reask_messages(raw_response, params, _config) do
    content =
      case raw_response do
        nil ->
          "Your previous response was not valid JSON. Please try again."

        text ->
          "Your previous response:\n#{text}\n\nPlease correct it and respond with valid JSON only."
      end

    messages = Keyword.get(params, :messages, [])
    messages ++ [%{role: "user", content: content}]
  end

  # ---------------------------------------------------------------------------

  defp session_name(config) do
    config = config || []

    Keyword.get(config, :session) ||
      (Application.get_env(:instructor, :turboquant_llm) || [])[:session] ||
      raise ArgumentError,
            "TurboquantLlm session not configured. " <>
              "Set config :instructor, turboquant_llm: [session: :name]"
  end

  defp build_messages(params) do
    messages = Keyword.get(params, :messages, [])
    response_format = Keyword.get(params, :response_format)

    schema_json =
      case response_format do
        %{json_schema: %{schema: schema}} -> Jason.encode!(schema, pretty: true)
        _ -> nil
      end

    if schema_json do
      messages ++
        [
          %{
            role: "user",
            content:
              "Respond with a JSON object exactly matching this schema:\n#{schema_json}\nJSON only, no other text."
          }
        ]
    else
      messages
    end
  end
end
