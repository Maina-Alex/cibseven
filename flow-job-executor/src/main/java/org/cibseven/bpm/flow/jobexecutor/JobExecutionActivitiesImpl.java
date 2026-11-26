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

import org.cibseven.bpm.engine.ManagementService;
import org.cibseven.bpm.engine.impl.interceptor.CommandExecutor;
import org.cibseven.bpm.engine.impl.jobexecutor.ExecuteJobHelper;

/**
 * Bound to one engine node's own {@link CommandExecutor}/{@link ManagementService} at
 * construction time, but reachable from any node's worker since all nodes share one task queue
 * (see {@link ClusterSafeJobExecutor}) - whichever node's worker happens to run this instance
 * executes the job against the same shared {@code ACT_RU_*} rows regardless of which node
 * originally claimed it.
 */
public class JobExecutionActivitiesImpl implements JobExecutionActivities {

  private final CommandExecutor commandExecutor;
  private final ManagementService managementService;

  public JobExecutionActivitiesImpl(CommandExecutor commandExecutor, ManagementService managementService) {
    this.commandExecutor = commandExecutor;
    this.managementService = managementService;
  }

  @Override
  public void executeJob(String jobId) {
    // Re-check existence before executing: a retried activity (ours or a previous attempt's)
    // may already have completed the job, in which case this call must safely no-op rather than
    // fail or re-execute a side effect.
    boolean stillExists = managementService.createJobQuery().jobId(jobId).singleResult() != null;
    if (!stillExists) {
      return;
    }
    ExecuteJobHelper.executeJob(jobId, commandExecutor);
  }

}
