Application.ensure_all_started(:propcheck)
ExUnit.start()
Mox.defmock(Taskweft.GEPA.InstructorMock, for: Instructor.Adapter)
