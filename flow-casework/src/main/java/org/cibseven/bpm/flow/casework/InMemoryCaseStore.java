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

import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A single-process {@link CaseStore}. A JPA-backed store is a separately-tracked open item, same
 * as {@code flow-repository}'s {@code InMemoryWorkflowVersionStore}.
 */
public class InMemoryCaseStore implements CaseStore {

  private final ConcurrentHashMap<String, Case> casesById = new ConcurrentHashMap<>();

  @Override
  public Case create(String title) {
    Case newCase = new Case(UUID.randomUUID().toString(), title, new Date());
    casesById.put(newCase.id(), newCase);
    return newCase;
  }

  @Override
  public Optional<Case> findById(String caseId) {
    return Optional.ofNullable(casesById.get(caseId));
  }

  @Override
  public Case close(String caseId) {
    Case existing = require(caseId);
    existing.close(new Date());
    return existing;
  }

  @Override
  public Case reopen(String caseId) {
    Case existing = require(caseId);
    existing.reopen();
    return existing;
  }

  private Case require(String caseId) {
    return findById(caseId).orElseThrow(() -> new IllegalArgumentException("No case with id '" + caseId + "'"));
  }

}
