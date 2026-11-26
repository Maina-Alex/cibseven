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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.cibseven.bpm.engine.repository.Deployment;
import org.cibseven.bpm.engine.repository.ProcessDefinition;
import org.cibseven.bpm.engine.runtime.ProcessInstance;
import org.cibseven.bpm.engine.test.util.PluggableProcessEngineTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class WorkflowDeploymentServiceTest extends PluggableProcessEngineTest {

  private static final String PROCESS_XML = simpleProcess("greetProcess");

  private final WorkflowVersionStore versionStore = new InMemoryWorkflowVersionStore();
  private WorkflowDeploymentService deploymentService;
  private Deployment deployment;
  private Deployment secondDeployment;

  @AfterEach
  void cleanUp() {
    if (deployment != null) {
      repositoryService.deleteDeployment(deployment.getId(), true);
    }
    if (secondDeployment != null) {
      repositoryService.deleteDeployment(secondDeployment.getId(), true);
    }
  }

  @Test
  void deploysTheCurrentlyPublishedVersionFromTheVersionStore() {
    deploymentService = new WorkflowDeploymentService(repositoryService, versionStore);

    versionStore.save(new WorkflowVersion("greetProcess", 1, "greetProcess.bpmn", PROCESS_XML, true));

    deployment = deploymentService.deployPublished("greetProcess");

    ProcessInstance instance = runtimeService.startProcessInstanceByKey("greetProcess");
    ProcessDefinition definition = repositoryService.createProcessDefinitionQuery()
        .processDefinitionId(instance.getProcessDefinitionId()).singleResult();
    assertThat(definition.getVersion()).isEqualTo(1);
    assertThat(deployment.getName()).isEqualTo("greetProcess");
  }

  @Test
  void deployingAgainAfterAPublishMovesToTheNewPublishedVersion() {
    deploymentService = new WorkflowDeploymentService(repositoryService, versionStore);

    versionStore.save(new WorkflowVersion("greetProcess", 1, "greetProcess.bpmn", PROCESS_XML, true));
    deployment = deploymentService.deployPublished("greetProcess");

    versionStore.save(new WorkflowVersion("greetProcess", 2, "greetProcess.bpmn", PROCESS_XML, true));
    secondDeployment = deploymentService.deployPublished("greetProcess");

    ProcessInstance instance = runtimeService.startProcessInstanceByKey("greetProcess");
    ProcessDefinition definition = repositoryService.createProcessDefinitionQuery()
        .processDefinitionId(instance.getProcessDefinitionId()).singleResult();
    assertThat(definition.getVersion()).isEqualTo(2);

    assertThat(versionStore.findPublished("greetProcess")).hasValueSatisfying(v -> assertThat(v.versionNumber()).isEqualTo(2));
    assertThat(versionStore.findByKeyAndVersion("greetProcess", 1)).hasValueSatisfying(v -> assertThat(v.published()).isFalse());
  }

  @Test
  void refusesToDeployAWorkflowKeyWithNoPublishedVersion() {
    deploymentService = new WorkflowDeploymentService(repositoryService, versionStore);

    assertThatThrownBy(() -> deploymentService.deployPublished("neverPublished"))
        .isInstanceOf(WorkflowDeploymentService.NoPublishedVersionException.class)
        .hasMessageContaining("neverPublished");
  }

  private static String simpleProcess(String processKey) {
    return """
        <?xml version="1.0" encoding="UTF-8"?>
        <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                     id="definitions"
                     targetNamespace="http://cibseven.org/flow/repository/test">
          <process id="%s" isExecutable="true">
            <startEvent id="start" />
            <sequenceFlow id="flow1" sourceRef="start" targetRef="end" />
            <endEvent id="end" />
          </process>
        </definitions>
        """.formatted(processKey);
  }

}
