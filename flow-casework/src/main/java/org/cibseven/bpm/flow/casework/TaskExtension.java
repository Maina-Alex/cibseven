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
package org.cibseven.bpm.flow.casework;

/**
 * Everything a case needs to know about a real CIB7 task that the task itself has no native
 * column for, keyed by the task's own id. {@code caseId}/{@code processInstanceId} are only
 * ever set together, through {@link CaseTaskLinkService#linkTask(String, String)}.
 */
public record TaskExtension(String taskId, String caseId, String processInstanceId,
    String recommendedDisposition, String finalDisposition) {

  public TaskExtension(String taskId) {
    this(taskId, null, null, null, null);
  }

  public TaskExtension withCase(String caseId, String processInstanceId) {
    return new TaskExtension(taskId, caseId, processInstanceId, recommendedDisposition, finalDisposition);
  }

  public TaskExtension withFinalDisposition(String finalDisposition) {
    return new TaskExtension(taskId, caseId, processInstanceId, recommendedDisposition, finalDisposition);
  }

}
