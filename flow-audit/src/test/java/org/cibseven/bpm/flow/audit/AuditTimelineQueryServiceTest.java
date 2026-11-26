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
package org.cibseven.bpm.flow.audit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.cibseven.bpm.engine.repository.Deployment;
import org.cibseven.bpm.engine.runtime.ProcessInstance;
import org.cibseven.bpm.engine.task.Task;
import org.cibseven.bpm.engine.test.util.PluggableProcessEngineTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AuditTimelineQueryServiceTest extends PluggableProcessEngineTest {

  private static final String PROCESS_XML = """
      <?xml version="1.0" encoding="UTF-8"?>
      <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                   xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                   id="definitions"
                   targetNamespace="http://cibseven.org/flow/audit/test">
        <process id="auditTimelineProcess" isExecutable="true">
          <startEvent id="start" />
          <sequenceFlow id="flow0" sourceRef="start" targetRef="setDirectly" />
          <serviceTask id="setDirectly" camunda:class="org.cibseven.bpm.flow.audit.DirectVariableSetDelegate" />
          <sequenceFlow id="flow1" sourceRef="setDirectly" targetRef="review" />
          <userTask id="review" name="Review" />
          <sequenceFlow id="flow2" sourceRef="review" targetRef="end" />
          <endEvent id="end" />
        </process>
      </definitions>
      """;

  private AuditTimelineQueryService timelineService;
  private Deployment deployment;

  @AfterEach
  void cleanUp() {
    identityService.clearAuthentication();
    if (deployment != null) {
      repositoryService.deleteDeployment(deployment.getId(), true);
    }
  }

  @Test
  void mergesVariableUpdatesTaskTransitionsAndCommentsIntoOneChronologicalTimeline() {
    timelineService = new AuditTimelineQueryService(historyService, taskService);

    deployment = repositoryService.createDeployment().addString("auditTimelineProcess.bpmn", PROCESS_XML).deploy();

    identityService.setAuthenticatedUserId("alice");
    // Reaches the DirectVariableSetDelegate service task before suspending at the user task.
    // That delegate sets "viaDelegate" via DelegateExecution.setVariable(...) mid-transaction,
    // with no top-level Command of its own, so no UserOperationLogEntry is logged for it.
    ProcessInstance instance = runtimeService.startProcessInstanceByKey("auditTimelineProcess");

    // A public-API call, routed through SetExecutionVariablesCmd, which DOES log one.
    runtimeService.setVariable(instance.getId(), "viaRuntimeService", "value-a");

    Task task = taskService.createTaskQuery().processInstanceId(instance.getId()).singleResult();
    taskService.claim(task.getId(), "alice");
    taskService.createComment(task.getId(), instance.getId(), "Looks fine to me");

    // Completing the task logs a "Complete" operation entry that the variable update it
    // carries can join against, so this one DOES resolve an actor.
    taskService.complete(task.getId(), Map.of("decision", "approved"));

    List<AuditTimelineEntry> timeline = timelineService.getTimeline(instance.getId());

    assertThat(timeline).isNotEmpty();
    assertThat(timeline).isSortedAccordingTo(java.util.Comparator.comparing(AuditTimelineEntry::timestamp));

    AuditTimelineEntry delegateSetUpdate = timeline.stream()
        .filter(e -> e.type() == AuditTimelineEntry.Type.VARIABLE_UPDATE && e.description().contains("viaDelegate"))
        .findFirst().orElseThrow();
    assertThat(delegateSetUpdate.actorId()).isNull();

    AuditTimelineEntry runtimeServiceUpdate = timeline.stream()
        .filter(e -> e.type() == AuditTimelineEntry.Type.VARIABLE_UPDATE && e.description().contains("viaRuntimeService"))
        .findFirst().orElseThrow();
    assertThat(runtimeServiceUpdate.actorId()).isEqualTo("alice");

    AuditTimelineEntry completeUpdate = timeline.stream()
        .filter(e -> e.type() == AuditTimelineEntry.Type.VARIABLE_UPDATE && e.description().contains("decision"))
        .findFirst().orElseThrow();
    assertThat(completeUpdate.actorId()).isEqualTo("alice");

    assertThat(timeline).anySatisfy(e -> {
      assertThat(e.type()).isEqualTo(AuditTimelineEntry.Type.COMMENT);
      assertThat(e.actorId()).isEqualTo("alice");
      assertThat(e.description()).isEqualTo("Looks fine to me");
    });

    assertThat(timeline).anySatisfy(e -> assertThat(e.type()).isEqualTo(AuditTimelineEntry.Type.TASK_CREATED));
    assertThat(timeline).anySatisfy(e -> {
      assertThat(e.type()).isEqualTo(AuditTimelineEntry.Type.TASK_COMPLETED);
      assertThat(e.actorId()).isEqualTo("alice");
    });
  }

}
