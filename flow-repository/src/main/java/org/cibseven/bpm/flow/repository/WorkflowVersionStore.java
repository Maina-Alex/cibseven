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
package org.cibseven.bpm.flow.repository;

import java.util.List;
import java.util.Optional;

/**
 * Governs versioned BPMN/DMN rows, independent of how they're actually persisted.
 * {@link WorkflowDeploymentService} depends only on this interface; a JPA-backed
 * implementation (the production store, backing draft/publish editing) can replace
 * {@link InMemoryWorkflowVersionStore} without either of those classes changing.
 */
public interface WorkflowVersionStore {

  WorkflowVersion save(WorkflowVersion version);

  Optional<WorkflowVersion> findPublished(String workflowKey);

  Optional<WorkflowVersion> findByKeyAndVersion(String workflowKey, int versionNumber);

  List<WorkflowVersion> findAllVersions(String workflowKey);

}
