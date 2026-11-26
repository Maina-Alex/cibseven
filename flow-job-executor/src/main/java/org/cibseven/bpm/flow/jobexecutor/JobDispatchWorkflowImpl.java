/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cibseven.bpm.flow.jobexecutor;

import java.time.Duration;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

public class JobDispatchWorkflowImpl implements JobDispatchWorkflow {

  private static final ActivityOptions ACTIVITY_OPTIONS = ActivityOptions.newBuilder()
      .setStartToCloseTimeout(Duration.ofSeconds(30))
      .setScheduleToStartTimeout(Duration.ofMinutes(5))
      .setRetryOptions(RetryOptions.newBuilder()
          .setInitialInterval(Duration.ofMillis(200))
          .setMaximumAttempts(5)
          .build())
      .build();

  private final JobExecutionActivities activities =
      Workflow.newActivityStub(JobExecutionActivities.class, ACTIVITY_OPTIONS);

  @Override
  public void runJob(String jobId) {
    activities.executeJob(jobId);
  }

}
