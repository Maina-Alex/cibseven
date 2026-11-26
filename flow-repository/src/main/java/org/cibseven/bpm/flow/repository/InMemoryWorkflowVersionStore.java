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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A single-process {@link WorkflowVersionStore}. Saving a version with {@code published=true}
 * demotes any previously published version of the same key, so at most one version per key is
 * published at a time.
 */
public class InMemoryWorkflowVersionStore implements WorkflowVersionStore {

  private final ConcurrentHashMap<String, List<WorkflowVersion>> versionsByKey = new ConcurrentHashMap<>();

  @Override
  public WorkflowVersion save(WorkflowVersion version) {
    List<WorkflowVersion> versions = versionsByKey.computeIfAbsent(version.workflowKey(), k -> new CopyOnWriteArrayList<>());
    versions.removeIf(v -> v.versionNumber() == version.versionNumber());
    if (version.published()) {
      versions.replaceAll(v -> v.published() ? demote(v) : v);
    }
    versions.add(version);
    return version;
  }

  @Override
  public Optional<WorkflowVersion> findPublished(String workflowKey) {
    return findAllVersions(workflowKey).stream().filter(WorkflowVersion::published).findFirst();
  }

  @Override
  public Optional<WorkflowVersion> findByKeyAndVersion(String workflowKey, int versionNumber) {
    return findAllVersions(workflowKey).stream().filter(v -> v.versionNumber() == versionNumber).findFirst();
  }

  @Override
  public List<WorkflowVersion> findAllVersions(String workflowKey) {
    return new ArrayList<>(versionsByKey.getOrDefault(workflowKey, List.of()));
  }

  private static WorkflowVersion demote(WorkflowVersion published) {
    return new WorkflowVersion(published.workflowKey(), published.versionNumber(), published.resourceName(), published.xml(), false);
  }

}
