# SPDX-License-Identifier: MIT
# Copyright (c) 2026 K. S. Ernest (iFire) Lee

defmodule SkillAllocation do
  use Taskweft.DSL

  @name "skill_allocation"

  @variables %{
    engineer_has_skill: %{type: :bool},
    job_skill_required: %{type: :ref},
    allocations: %{type: :ref},
    training_used: %{type: :int, init: %{total: 0}}
  }

  @actions %{
    assign_job: %{
      params: [:job_id, :engineer_id],
      body: [
        %{pointer_set: "/allocations/{job_id}", value: "{engineer_id}"}
      ]
    },

    train_engineer: %{
      params: [:engineer_id, :job_id],
      bind: [%{name: :skill, pointer: "/job_skill_required/{job_id}"}],
      body: [
        %{pointer_set: "/engineer_has_skill/{engineer_id}_{skill}", value: true}
      ]
    }
  }

  @methods %{
    allocate_job: %{
      params: [:job_id, :engineer_id],
      alternatives: [
        %{
          name: :assign_to_qualified,
          check: [
            %{eval: %{type: "math/eq", a: %{pointer_get: "/engineer_has_skill/{engineer_id}_{skill}"}, b: true}}
          ],
          subtasks: [[:assign_job, :"{job_id}", :"{engineer_id}"]]
        },
        %{
          name: :train_and_assign,
          subtasks: [
            [:train_engineer, :"{engineer_id}", :"{job_id}"],
            [:assign_job, :"{job_id}", :"{engineer_id}"]
          ]
        }
      ]
    }
  }
end
