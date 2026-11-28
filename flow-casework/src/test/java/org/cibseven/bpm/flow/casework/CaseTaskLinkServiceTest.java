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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Comparator;
import java.util.List;

import org.cibseven.bpm.engine.repository.Deployment;
import org.cibseven.bpm.engine.runtime.ProcessInstance;
import org.cibseven.bpm.engine.task.Task;
import org.cibseven.bpm.engine.test.util.PluggableProcessEngineTest;
import org.cibseven.bpm.flow.audit.AuditTimelineEntry;
import org.cibseven.bpm.flow.audit.AuditTimelineQueryService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class CaseTaskLinkServiceTest extends PluggableProcessEngineTest {

  private static final String PROCESS_XML = """
      <?xml version="1.0" encoding="UTF-8"?>
      <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                   id="definitions"
                   targetNamespace="http://cibseven.org/flow/casework/test">
        <process id="caseworkProcess" isExecutable="true">
          <startEvent id="start" />
          <sequenceFlow id="flow1" sourceRef="start" targetRef="review" />
          <userTask id="review" name="Review" />
          <sequenceFlow id="flow2" sourceRef="review" targetRef="end" />
          <endEvent id="end" />
        </process>
      </definitions>
      """;

  private final CaseEventStore caseEventStore = new CaseEventStore();
  private final TaskExtensionStore taskExtensionStore = new InMemoryTaskExtensionStore();
  private final CaseService caseService = new CaseService(new InMemoryCaseStore(), caseEventStore);

  private Deployment deployment;

  @AfterEach
  void cleanUp() {
    identityService.clearAuthentication();
    if (deployment != null) {
      repositoryService.deleteDeployment(deployment.getId(), true);
    }
  }

  @Test
  void aCaseOwnsABpmnTaskThroughItsWholeLifecycleAndOutlivesItsCompletion() {
    CaseTaskLinkService linkService = new CaseTaskLinkService(taskService, identityService, taskExtensionStore, caseEventStore);
    CaseTimelineQueryService timelineService = new CaseTimelineQueryService(
        new AuditTimelineQueryService(historyService, taskService), caseEventStore, taskExtensionStore);

    deployment = repositoryService.createDeployment().addString("caseworkProcess.bpmn", PROCESS_XML).deploy();

    Case investigation = caseService.create("Investigate thing");

    identityService.setAuthenticatedUserId("alice");
    ProcessInstance instance = runtimeService.startProcessInstanceByKey("caseworkProcess");
    Task task = taskService.createTaskQuery().processInstanceId(instance.getId()).singleResult();

    TaskExtension linked = linkService.linkTask(investigation.id(), task.getId());
    assertThat(linked.caseId()).isEqualTo(investigation.id());
    assertThat(linked.processInstanceId()).isEqualTo(instance.getId());

    List<AuditTimelineEntry> afterLink = timelineService.getTimeline(investigation.id());
    assertThat(afterLink).anySatisfy(e -> {
      assertThat(e.type()).isEqualTo(AuditTimelineEntry.Type.CASE_EVENT);
      assertThat(e.description()).contains("linked to case");
      assertThat(e.actorId()).isEqualTo("alice");
    });

    TaskExtension completed = linkService.completeWithDisposition(task.getId(), "approved");
    assertThat(completed.finalDisposition()).isEqualTo("approved");

    assertThat(runtimeService.createProcessInstanceQuery().processInstanceId(instance.getId()).singleResult()).isNull();

    List<AuditTimelineEntry> timeline = timelineService.getTimeline(investigation.id());
    assertThat(timeline).isSortedAccordingTo(Comparator.comparing(AuditTimelineEntry::timestamp));

    assertThat(timeline).anySatisfy(e -> {
      assertThat(e.type()).isEqualTo(AuditTimelineEntry.Type.CASE_EVENT);
      assertThat(e.description()).contains("completed with disposition 'approved'");
      assertThat(e.actorId()).isEqualTo("alice");
    });
    // flow-audit's own native entries for the linked process instance are merged in too, not
    // reimplemented: the task-completion transition and the finalDisposition variable write.
    assertThat(timeline).anySatisfy(e -> assertThat(e.type()).isEqualTo(AuditTimelineEntry.Type.TASK_COMPLETED));
    assertThat(timeline).anySatisfy(e -> {
      assertThat(e.type()).isEqualTo(AuditTimelineEntry.Type.VARIABLE_UPDATE);
      assertThat(e.description()).contains("finalDisposition");
    });

    // A case does not auto-close just because one task on it completed: it outlives the execution.
    assertThat(investigation.status()).isEqualTo(Case.Status.OPEN);
  }

  @Test
  void closingAndReopeningACaseAreBothRecordedOnItsTimeline() {
    CaseTimelineQueryService timelineService = new CaseTimelineQueryService(
        new AuditTimelineQueryService(historyService, taskService), caseEventStore, taskExtensionStore);

    Case investigation = caseService.create("Investigate thing");

    caseService.close(investigation.id());
    assertThat(investigation.status()).isEqualTo(Case.Status.CLOSED);
    assertThat(timelineService.getTimeline(investigation.id()))
        .anySatisfy(e -> assertThat(e.description()).contains("Case closed"));

    caseService.reopen(investigation.id());
    assertThat(investigation.status()).isEqualTo(Case.Status.REOPENED);
    assertThat(timelineService.getTimeline(investigation.id()))
        .anySatisfy(e -> assertThat(e.description()).contains("Case reopened"));
  }

}
