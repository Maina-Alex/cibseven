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

import org.cibseven.bpm.engine.RepositoryService;
import org.cibseven.bpm.engine.repository.Deployment;

/**
 * Reads the currently published {@link WorkflowVersion} for a key out of a
 * {@link WorkflowVersionStore} and deploys its XML via
 * {@code RepositoryService.createDeployment().addString(...).deploy()} — replacing CIBseven's
 * default file/classpath/zip-based deployment with one driven by Flow's own versioned rows.
 */
public class WorkflowDeploymentService {

  private final RepositoryService repositoryService;
  private final WorkflowVersionStore versionStore;

  public WorkflowDeploymentService(RepositoryService repositoryService, WorkflowVersionStore versionStore) {
    this.repositoryService = repositoryService;
    this.versionStore = versionStore;
  }

  public Deployment deployPublished(String workflowKey) {
    WorkflowVersion published = versionStore.findPublished(workflowKey)
        .orElseThrow(() -> new NoPublishedVersionException(workflowKey));

    return repositoryService.createDeployment()
        .name(workflowKey)
        .addString(published.resourceName(), published.xml())
        .deploy();
  }

  public static class NoPublishedVersionException extends RuntimeException {

    public NoPublishedVersionException(String workflowKey) {
      super("No published version found for workflow key '" + workflowKey + "'");
    }

  }

}
