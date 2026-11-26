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

/**
 * One versioned row of a BPMN/DMN resource: a {@code workflowKey} (the process/decision
 * definition key), a monotonically increasing {@code versionNumber}, the authored XML, and
 * whether this version is the one currently published. Mirrors the draft/publish versioning
 * Flow already governs workflows through today (outside the engine), now modeled natively.
 */
public record WorkflowVersion(String workflowKey, int versionNumber, String resourceName, String xml, boolean published) {

  public WorkflowVersion {
    if (workflowKey == null || workflowKey.isBlank()) {
      throw new IllegalArgumentException("workflowKey must not be blank");
    }
    if (resourceName == null || resourceName.isBlank()) {
      throw new IllegalArgumentException("resourceName must not be blank");
    }
    if (xml == null || xml.isBlank()) {
      throw new IllegalArgumentException("xml must not be blank");
    }
  }

  public WorkflowVersion asPublished() {
    return new WorkflowVersion(workflowKey, versionNumber, resourceName, xml, true);
  }

}
