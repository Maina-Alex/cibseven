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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.cibseven.bpm.flow.audit.AuditTimelineEntry;
import org.cibseven.bpm.flow.audit.AuditTimelineQueryService;

/**
 * A case's timeline merges {@code flow-audit}'s per-process-instance timeline across every
 * distinct process instance this case's linked tasks belong to, with this case's own
 * {@link AuditTimelineEntry.Type#CASE_EVENT} entries — proving a case's audit trail can span
 * multiple executions, not just one, by reusing flow-audit's merge rather than duplicating it.
 */
public class CaseTimelineQueryService {

  private final AuditTimelineQueryService auditTimelineQueryService;
  private final CaseEventStore caseEventStore;
  private final TaskExtensionStore taskExtensionStore;

  public CaseTimelineQueryService(AuditTimelineQueryService auditTimelineQueryService,
      CaseEventStore caseEventStore, TaskExtensionStore taskExtensionStore) {
    this.auditTimelineQueryService = auditTimelineQueryService;
    this.caseEventStore = caseEventStore;
    this.taskExtensionStore = taskExtensionStore;
  }

  public List<AuditTimelineEntry> getTimeline(String caseId) {
    List<AuditTimelineEntry> entries = new ArrayList<>(caseEventStore.findByCaseId(caseId));

    Set<String> processInstanceIds = new HashSet<>();
    for (TaskExtension extension : taskExtensionStore.findByCaseId(caseId)) {
      if (extension.processInstanceId() != null) {
        processInstanceIds.add(extension.processInstanceId());
      }
    }
    for (String processInstanceId : processInstanceIds) {
      entries.addAll(auditTimelineQueryService.getTimeline(processInstanceId));
    }

    entries.sort(Comparator.comparing(AuditTimelineEntry::timestamp));
    return entries;
  }

}
