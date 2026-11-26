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

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Runs on whichever node's worker is polling the shared task queue when the activity task is
 * dispatched - not necessarily the node that claimed the job. This is the mechanism that keeps a
 * dispatching node's crash from orphaning the job: a surviving node's worker, polling the same
 * queue, picks up the retry.
 */
@ActivityInterface
public interface JobExecutionActivities {

  @ActivityMethod
  void executeJob(String jobId);

}
