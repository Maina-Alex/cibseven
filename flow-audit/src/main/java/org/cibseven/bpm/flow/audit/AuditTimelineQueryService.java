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
package org.cibseven.bpm.flow.audit;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.cibseven.bpm.engine.HistoryService;
import org.cibseven.bpm.engine.TaskService;
import org.cibseven.bpm.engine.history.HistoricActivityInstance;
import org.cibseven.bpm.engine.history.HistoricDetail;
import org.cibseven.bpm.engine.history.HistoricVariableUpdate;
import org.cibseven.bpm.engine.history.UserOperationLogEntry;
import org.cibseven.bpm.engine.task.Comment;

/**
 * Merges History's variable updates and task transitions with task comments into one
 * chronological per-process-instance timeline. History doesn't hand this over as a single
 * feed, so this is the one read-side join point: callers get a complete audit trail for a
 * process instance without knowing where each piece of it actually lives.
 */
public class AuditTimelineQueryService {

  private final HistoryService historyService;
  private final TaskService taskService;

  public AuditTimelineQueryService(HistoryService historyService, TaskService taskService) {
    this.historyService = historyService;
    this.taskService = taskService;
  }

  public List<AuditTimelineEntry> getTimeline(String processInstanceId) {
    List<AuditTimelineEntry> entries = new ArrayList<>();
    entries.addAll(variableUpdateEntries(processInstanceId));
    entries.addAll(taskTransitionEntries(processInstanceId));
    entries.addAll(commentEntries(processInstanceId));
    entries.sort(Comparator.comparing(AuditTimelineEntry::timestamp));
    return entries;
  }

  private List<AuditTimelineEntry> variableUpdateEntries(String processInstanceId) {
    List<HistoricDetail> details = historyService.createHistoricDetailQuery()
        .processInstanceId(processInstanceId)
        .variableUpdates()
        .list();

    List<AuditTimelineEntry> entries = new ArrayList<>();
    for (HistoricDetail detail : details) {
      HistoricVariableUpdate update = (HistoricVariableUpdate) detail;
      entries.add(new AuditTimelineEntry(
          update.getTime(),
          resolveActorId(update.getUserOperationId()),
          AuditTimelineEntry.Type.VARIABLE_UPDATE,
          "Variable '" + update.getVariableName() + "' set to '" + update.getValue() + "'"));
    }
    return entries;
  }

  private List<AuditTimelineEntry> taskTransitionEntries(String processInstanceId) {
    List<HistoricActivityInstance> taskInstances = historyService.createHistoricActivityInstanceQuery()
        .processInstanceId(processInstanceId)
        .activityType("userTask")
        .list();

    List<AuditTimelineEntry> entries = new ArrayList<>();
    for (HistoricActivityInstance taskInstance : taskInstances) {
      entries.add(new AuditTimelineEntry(
          taskInstance.getStartTime(),
          null,
          AuditTimelineEntry.Type.TASK_CREATED,
          "Task '" + taskInstance.getActivityName() + "' created"));
      if (taskInstance.getEndTime() != null) {
        entries.add(new AuditTimelineEntry(
            taskInstance.getEndTime(),
            taskInstance.getAssignee(),
            AuditTimelineEntry.Type.TASK_COMPLETED,
            "Task '" + taskInstance.getActivityName() + "' completed"));
      }
    }
    return entries;
  }

  private List<AuditTimelineEntry> commentEntries(String processInstanceId) {
    List<Comment> comments = taskService.getProcessInstanceComments(processInstanceId);
    List<AuditTimelineEntry> entries = new ArrayList<>();
    for (Comment comment : comments) {
      entries.add(new AuditTimelineEntry(
          comment.getTime(),
          comment.getUserId(),
          AuditTimelineEntry.Type.COMMENT,
          comment.getFullMessage()));
    }
    return entries;
  }

  private String resolveActorId(String userOperationId) {
    if (userOperationId == null) {
      return null;
    }
    return historyService.createUserOperationLogQuery()
        .operationId(userOperationId)
        .list()
        .stream()
        .findFirst()
        .map(UserOperationLogEntry::getUserId)
        .orElse(null);
  }

}
