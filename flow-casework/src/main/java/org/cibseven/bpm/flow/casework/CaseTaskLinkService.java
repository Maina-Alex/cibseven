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

import java.util.Map;

import org.cibseven.bpm.engine.IdentityService;
import org.cibseven.bpm.engine.ProcessEngineException;
import org.cibseven.bpm.engine.TaskService;
import org.cibseven.bpm.engine.task.Task;

/**
 * The one choke point for attaching a real CIB7 task to a case. Writing the link without
 * recording it on the case's timeline — or vice versa — would produce a task the case cannot
 * see, or a timeline entry the case cannot act on, so both writes always happen together here;
 * nothing else should set {@link TaskExtension#caseId()} directly.
 */
public class CaseTaskLinkService {

  private final TaskService taskService;
  private final IdentityService identityService;
  private final TaskExtensionStore taskExtensionStore;
  private final CaseEventStore caseEventStore;

  public CaseTaskLinkService(TaskService taskService, IdentityService identityService,
      TaskExtensionStore taskExtensionStore, CaseEventStore caseEventStore) {
    this.taskService = taskService;
    this.identityService = identityService;
    this.taskExtensionStore = taskExtensionStore;
    this.caseEventStore = caseEventStore;
  }

  public TaskExtension linkTask(String caseId, String taskId) {
    Task task = requireTask(taskId);

    TaskExtension updated = taskExtensionStore.getOrCreate(taskId).withCase(caseId, task.getProcessInstanceId());
    taskExtensionStore.save(updated);

    caseEventStore.record(caseId, currentActorId(),
        "Task '" + task.getName() + "' (" + taskId + ") linked to case");
    return updated;
  }

  public TaskExtension completeWithDisposition(String taskId, String disposition) {
    Task task = requireTask(taskId);

    taskService.complete(taskId, Map.of("finalDisposition", disposition));

    TaskExtension updated = taskExtensionStore.getOrCreate(taskId).withFinalDisposition(disposition);
    taskExtensionStore.save(updated);

    if (updated.caseId() != null) {
      caseEventStore.record(updated.caseId(), currentActorId(),
          "Task '" + task.getName() + "' (" + taskId + ") completed with disposition '" + disposition + "'");
    }
    return updated;
  }

  private Task requireTask(String taskId) {
    Task task = taskService.createTaskQuery().taskId(taskId).singleResult();
    if (task == null) {
      throw new ProcessEngineException("No task with id '" + taskId + "'");
    }
    return task;
  }

  private String currentActorId() {
    return identityService.getCurrentAuthentication() != null
        ? identityService.getCurrentAuthentication().getUserId()
        : null;
  }

}
