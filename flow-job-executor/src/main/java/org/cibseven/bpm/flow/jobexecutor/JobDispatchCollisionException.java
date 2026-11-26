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

/**
 * Thrown by {@link ClusterSafeJobExecutor} when a Temporal {@code WorkflowExecutionAlreadyStarted}
 * cannot be confirmed as this job's own legitimate retry/dedupe. Swallowing every such conflict
 * uniformly was the exact defect that let a job silently never execute
 * (docs/cib7-fork-engine-plan.md §6 / §10 risk 12): a loud failure here is deliberately preferred
 * over a silent no-op that leaves the job stuck forever with no error anywhere.
 */
public class JobDispatchCollisionException extends RuntimeException {

  public JobDispatchCollisionException(String jobId, String workflowId, String conflictingJobId) {
    super("Refusing to silently swallow a Temporal workflow-id collision: job '" + jobId
        + "' derived workflow id '" + workflowId + "', but an existing workflow under that id is "
        + "recorded (via its memo) as belonging to job '" + conflictingJobId + "' instead. This is "
        + "not this job's own retry - dispatching it as if it were would leave job '" + jobId
        + "' stuck unexecuted. See docs/cib7-fork-engine-plan.md §10 risk 12.");
  }

  public JobDispatchCollisionException(String jobId, String workflowId, Throwable cause) {
    super("Refusing to silently swallow a Temporal workflow-id collision for job '" + jobId
        + "' (derived workflow id '" + workflowId + "'): the existing workflow under that id could "
        + "not be inspected to confirm it is this job's own retry, so it must not be assumed safe "
        + "to drop. See docs/cib7-fork-engine-plan.md §10 risk 12.", cause);
  }

}
