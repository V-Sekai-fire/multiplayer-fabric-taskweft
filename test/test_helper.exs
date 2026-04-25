Application.ensure_all_started(:propcheck)
ExUnit.start()
Code.require_file("support/db_helpers.ex", __DIR__)
