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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** A single-process {@link TaskExtensionStore}; a JPA-backed store is a separate open item. */
public class InMemoryTaskExtensionStore implements TaskExtensionStore {

  private final ConcurrentHashMap<String, TaskExtension> extensionsByTaskId = new ConcurrentHashMap<>();

  @Override
  public TaskExtension getOrCreate(String taskId) {
    return extensionsByTaskId.computeIfAbsent(taskId, TaskExtension::new);
  }

  @Override
  public Optional<TaskExtension> findByTaskId(String taskId) {
    return Optional.ofNullable(extensionsByTaskId.get(taskId));
  }

  @Override
  public TaskExtension save(TaskExtension extension) {
    extensionsByTaskId.put(extension.taskId(), extension);
    return extension;
  }

  @Override
  public List<TaskExtension> findByCaseId(String caseId) {
    List<TaskExtension> result = new ArrayList<>();
    for (TaskExtension extension : extensionsByTaskId.values()) {
      if (caseId.equals(extension.caseId())) {
        result.add(extension);
      }
    }
    return result;
  }

}
