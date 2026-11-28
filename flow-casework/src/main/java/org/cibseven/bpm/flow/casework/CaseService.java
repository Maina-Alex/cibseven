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

/** Thin lifecycle wrapper over {@link CaseStore} that records every transition on the timeline. */
public class CaseService {

  private final CaseStore caseStore;
  private final CaseEventStore caseEventStore;

  public CaseService(CaseStore caseStore, CaseEventStore caseEventStore) {
    this.caseStore = caseStore;
    this.caseEventStore = caseEventStore;
  }

  public Case create(String title) {
    Case created = caseStore.create(title);
    caseEventStore.record(created.id(), null, "Case '" + title + "' created");
    return created;
  }

  public Case close(String caseId) {
    Case closed = caseStore.close(caseId);
    caseEventStore.record(caseId, null, "Case closed");
    return closed;
  }

  public Case reopen(String caseId) {
    Case reopened = caseStore.reopen(caseId);
    caseEventStore.record(caseId, null, "Case reopened");
    return reopened;
  }

}
