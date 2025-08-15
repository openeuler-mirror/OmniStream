E2E test
This E2E test utilizes a POD (Plain Old Data) JSON representation of job, task, and TDD information to define a task. 
The test scenario is then applied to this constructed task. The SUT includes the TaskExecutor, TaskManagerService, 
and OmniTask entry classes, and the test aims to validate the behavior of the complete system, including all underlying components.