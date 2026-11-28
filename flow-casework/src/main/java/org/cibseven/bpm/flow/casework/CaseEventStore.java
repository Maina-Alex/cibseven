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
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.cibseven.bpm.flow.audit.AuditTimelineEntry;

/**
 * Case-level events (created, task linked, task completed, closed, reopened) as
 * {@link AuditTimelineEntry.Type#CASE_EVENT} rows, kept separately from flow-audit's own
 * per-process-instance store so a case's timeline can merge both without flow-audit needing to
 * know cases exist.
 */
public class CaseEventStore {

  private final ConcurrentHashMap<String, List<AuditTimelineEntry>> entriesByCaseId = new ConcurrentHashMap<>();

  public void record(String caseId, String actorId, String description) {
    record(caseId, new Date(), actorId, description);
  }

  public void record(String caseId, Date timestamp, String actorId, String description) {
    entriesByCaseId.computeIfAbsent(caseId, k -> new CopyOnWriteArrayList<>())
        .add(new AuditTimelineEntry(timestamp, actorId, AuditTimelineEntry.Type.CASE_EVENT, description));
  }

  public List<AuditTimelineEntry> findByCaseId(String caseId) {
    return new ArrayList<>(entriesByCaseId.getOrDefault(caseId, List.of()));
  }

}
