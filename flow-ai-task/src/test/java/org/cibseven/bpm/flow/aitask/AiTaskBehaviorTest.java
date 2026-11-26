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
package org.cibseven.bpm.flow.aitask;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.cibseven.bpm.engine.repository.Deployment;
import org.cibseven.bpm.engine.runtime.ProcessInstance;
import org.cibseven.bpm.engine.task.Task;
import org.cibseven.bpm.engine.test.util.PluggableProcessEngineTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AiTaskBehaviorTest extends PluggableProcessEngineTest {

  private static final String PROCESS_XML = """
      <?xml version="1.0" encoding="UTF-8"?>
      <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                   xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   id="definitions"
                   targetNamespace="http://cibseven.org/flow/aitask/test">
        <process id="aiTaskProcess" isExecutable="true">
          <startEvent id="start" />
          <sequenceFlow id="flow1" sourceRef="start" targetRef="aiTask" />
          <serviceTask id="aiTask" camunda:class="org.cibseven.bpm.flow.aitask.AiTaskBehavior">
            <extensionElements>
              <camunda:properties>
                <camunda:property name="sopPrompt" value="Decide whether to approve the request." />
                <camunda:property name="toolPaths" value="" />
                <camunda:property name="confidenceThreshold" value="0.8" />
                <camunda:property name="handoverQueue" value="reviewQueue" />
              </camunda:properties>
            </extensionElements>
          </serviceTask>
          <sequenceFlow id="flow2" sourceRef="aiTask" targetRef="gateway" />
          <exclusiveGateway id="gateway" />
          <sequenceFlow id="toAutoResolve" sourceRef="gateway" targetRef="autoResolveEnd">
            <conditionExpression xsi:type="tFormalExpression">${aiConfidenceMet == true}</conditionExpression>
          </sequenceFlow>
          <sequenceFlow id="toHandover" sourceRef="gateway" targetRef="handoverTask">
            <conditionExpression xsi:type="tFormalExpression">${aiConfidenceMet == false}</conditionExpression>
          </sequenceFlow>
          <userTask id="handoverTask" name="Confirm AI recommendation" camunda:candidateGroups="${handoverQueue}" />
          <sequenceFlow id="flow3" sourceRef="handoverTask" targetRef="handoverEnd" />
          <endEvent id="autoResolveEnd" />
          <endEvent id="handoverEnd" />
        </process>
      </definitions>
      """;

  private Deployment deployment;

  @BeforeEach
  void wireFakeExecutor() {
    AiTaskBehavior.setExecutor(new FakeAiTaskExecutor());
  }

  @AfterEach
  void cleanUp() {
    if (deployment != null) {
      repositoryService.deleteDeployment(deployment.getId(), true);
    }
  }

  @Test
  void autoResolvesWhenConfidenceMeetsTheThreshold() {
    deployment = repositoryService.createDeployment().addString("aiTaskProcess.bpmn", PROCESS_XML).deploy();

    ProcessInstance instance = runtimeService.startProcessInstanceByKey("aiTaskProcess", Map.of("forcedConfidence", 0.95));

    assertThat(runtimeService.createProcessInstanceQuery().processInstanceId(instance.getId()).singleResult()).isNull();
    assertThat(taskService.createTaskQuery().processInstanceId(instance.getId()).count()).isZero();

    Object recommendedDisposition = historyService.createHistoricVariableInstanceQuery()
        .processInstanceId(instance.getId()).variableName("recommendedDisposition").singleResult().getValue();
    assertThat(recommendedDisposition).isEqualTo("approve");
  }

  @Test
  void handsOverToTheConfiguredQueueWhenConfidenceFallsShort() {
    deployment = repositoryService.createDeployment().addString("aiTaskProcess.bpmn", PROCESS_XML).deploy();

    ProcessInstance instance = runtimeService.startProcessInstanceByKey("aiTaskProcess", Map.of("forcedConfidence", 0.3));

    Task handoverTask = taskService.createTaskQuery().processInstanceId(instance.getId()).singleResult();
    assertThat(handoverTask).isNotNull();
    assertThat(handoverTask.getName()).isEqualTo("Confirm AI recommendation");

    assertThat(taskService.createTaskQuery().taskId(handoverTask.getId()).taskCandidateGroup("reviewQueue").count()).isEqualTo(1);

    assertThat(runtimeService.getVariable(instance.getId(), "recommendedDisposition")).isEqualTo("approve");
    assertThat(runtimeService.getVariable(instance.getId(), "aiConfidenceMet")).isEqualTo(false);
  }

}
