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
package org.cibseven.bpm.flow.tools;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.cibseven.bpm.engine.ProcessEngineException;
import org.cibseven.bpm.engine.repository.Deployment;
import org.cibseven.bpm.engine.runtime.ProcessInstance;
import org.cibseven.bpm.engine.test.util.PluggableProcessEngineTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ToolDelegateTest extends PluggableProcessEngineTest {

  private static final String PROCESS_XML = """
      <?xml version="1.0" encoding="UTF-8"?>
      <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                   xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                   id="definitions"
                   targetNamespace="http://cibseven.org/flow/tools/test">
        <process id="toolDelegateProcess" isExecutable="true">
          <startEvent id="start" />
          <sequenceFlow id="flow1" sourceRef="start" targetRef="callTool" />
          <serviceTask id="callTool" camunda:class="org.cibseven.bpm.flow.tools.ToolDelegate">
            <extensionElements>
              <camunda:field name="toolPath">
                <camunda:string>math/add</camunda:string>
              </camunda:field>
              <camunda:field name="resultVariable">
                <camunda:string>sumResult</camunda:string>
              </camunda:field>
            </extensionElements>
          </serviceTask>
          <sequenceFlow id="flow2" sourceRef="callTool" targetRef="end" />
          <endEvent id="end" />
        </process>
      </definitions>
      """;

  private Deployment deployment;

  @BeforeEach
  void registerTool() {
    ToolRegistry.getDefault().register(new AddNumbersTool());
  }

  @AfterEach
  void cleanUp() {
    ToolRegistry.getDefault().clear();
    if (deployment != null) {
      repositoryService.deleteDeployment(deployment.getId(), true);
    }
  }

  @Test
  void resolvesAndInvokesACodeRegisteredToolByPath() {
    deployment = repositoryService.createDeployment()
        .addString("toolDelegateProcess.bpmn", PROCESS_XML)
        .deploy();

    ProcessInstance instance = runtimeService.startProcessInstanceByKey(
        "toolDelegateProcess", Map.of("a", 2, "b", 3.5));

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) historyService.createHistoricVariableInstanceQuery()
        .processInstanceId(instance.getId())
        .variableName("sumResult")
        .singleResult()
        .getValue();

    assertThat(result).containsEntry("sum", 5.5);
  }

  @Test
  void failsClearlyWhenNoToolIsRegisteredAtThatPath() {
    ToolRegistry.getDefault().clear();
    deployment = repositoryService.createDeployment()
        .addString("toolDelegateProcess.bpmn", PROCESS_XML)
        .deploy();

    assertThatThrownBy(() -> runtimeService.startProcessInstanceByKey("toolDelegateProcess", Map.of("a", 1, "b", 1)))
        .isInstanceOf(ProcessEngineException.class)
        .hasMessageContaining("No tool registered at path 'math/add'");
  }

}
