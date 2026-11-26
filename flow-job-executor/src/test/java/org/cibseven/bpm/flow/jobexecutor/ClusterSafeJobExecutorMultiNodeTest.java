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
package org.cibseven.bpm.flow.jobexecutor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.cibseven.bpm.engine.ManagementService;
import org.cibseven.bpm.engine.ProcessEngine;
import org.cibseven.bpm.engine.ProcessEngineConfiguration;
import org.cibseven.bpm.engine.RepositoryService;
import org.cibseven.bpm.engine.RuntimeService;
import org.cibseven.bpm.engine.impl.cfg.StandaloneInMemProcessEngineConfiguration;
import org.cibseven.bpm.engine.runtime.Job;
import org.cibseven.bpm.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;

/**
 * Proves the fix for docs/cib7-fork-engine-plan.md §10 risk 12 end to end, against a real local
 * Temporal 1.29 server (no fake/stub Temporal client): two independent {@link ProcessEngine}
 * instances ("node A"/"node B") sharing one database and one Temporal task queue, exactly the
 * topology the risk describes.
 *
 * <p>Each test builds its own pair of engines against a uniquely-named H2 in-memory database, so
 * the two tests never share state.
 */
class ClusterSafeJobExecutorMultiNodeTest {

  private static final String PROCESS_XML = """
      <?xml version="1.0" encoding="UTF-8"?>
      <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                   xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                   id="definitions"
                   targetNamespace="http://cibseven.org/flow/jobexecutor/test">
        <process id="asyncJobProcess" isExecutable="true">
          <startEvent id="start" />
          <sequenceFlow id="f1" sourceRef="start" targetRef="asyncTask" />
          <serviceTask id="asyncTask" camunda:asyncBefore="true"
                       camunda:class="org.cibseven.bpm.flow.jobexecutor.NoopServiceDelegate" />
          <sequenceFlow id="f2" sourceRef="asyncTask" targetRef="end" />
          <endEvent id="end" />
        </process>
      </definitions>
      """;

  private ProcessEngine engineA;
  private ProcessEngine engineB;
  private ClusterSafeJobExecutor executorA;
  private ClusterSafeJobExecutor executorB;
  private WorkflowServiceStubs temporalServiceStubs;

  @AfterEach
  void tearDown() {
    if (executorA != null) {
      executorA.shutdown();
    }
    if (executorB != null) {
      executorB.shutdown();
    }
    if (engineA != null) {
      engineA.close();
    }
    if (engineB != null) {
      engineB.close();
    }
    if (temporalServiceStubs != null) {
      temporalServiceStubs.shutdownNow();
    }
  }

  @Test
  void onlyOneNodeClaimsAJobBothAreRacingFor() throws Exception {
    String dbName = "claim-race-" + UUID.randomUUID();
    String taskQueue = "flow-job-executor-test-" + UUID.randomUUID();
    WorkflowClient workflowClient = newWorkflowClient();

    executorA = new ClusterSafeJobExecutor(workflowClient, taskQueue, Long.MAX_VALUE / 2);
    executorB = new ClusterSafeJobExecutor(workflowClient, taskQueue, Long.MAX_VALUE / 2);
    // Both engines built with jobExecutorActivate=false: the job gets created below while neither
    // executor is active, so nothing auto-claims it via jobWasAdded() before the race starts.
    engineA = buildEngine(dbName, "node-A-" + dbName, executorA);
    engineB = buildEngine(dbName, "node-B-" + dbName, executorB);

    RepositoryService repositoryService = engineA.getRepositoryService();
    repositoryService.createDeployment().addString("asyncJobProcess.bpmn", PROCESS_XML).deploy();

    RuntimeService runtimeService = engineA.getRuntimeService();
    ProcessInstance instance = runtimeService.startProcessInstanceByKey("asyncJobProcess");

    ManagementService managementServiceA = engineA.getManagementService();
    Job job = managementServiceA.createJobQuery().processInstanceId(instance.getId()).singleResult();
    assertThat(job).as("asyncBefore should have created exactly one pending job").isNotNull();
    String jobId = job.getId();

    // Now bring both nodes' Temporal workers up and race them for this one, already-existing job.
    executorA.start();
    executorB.start();

    CyclicBarrier barrier = new CyclicBarrier(2);
    ExecutorService race = Executors.newFixedThreadPool(2);
    try {
      Callable<List<String>> raceOnA = () -> {
        barrier.await(10, TimeUnit.SECONDS);
        return executorA.triggerAcquisitionCycle();
      };
      Callable<List<String>> raceOnB = () -> {
        barrier.await(10, TimeUnit.SECONDS);
        return executorB.triggerAcquisitionCycle();
      };
      Future<List<String>> futureA = race.submit(raceOnA);
      Future<List<String>> futureB = race.submit(raceOnB);

      List<String> claimedByA = futureA.get(10, TimeUnit.SECONDS);
      List<String> claimedByB = futureB.get(10, TimeUnit.SECONDS);

      boolean aClaimedIt = claimedByA.contains(jobId);
      boolean bClaimedIt = claimedByB.contains(jobId);

      assertThat(aClaimedIt ^ bClaimedIt)
          .as("exactly one of the two racing nodes should have won AcquireJobsCmd's optimistic lock for job %s"
              + " (A claimed=%s, B claimed=%s)", jobId, aClaimedIt, bClaimedIt)
          .isTrue();
    } finally {
      race.shutdownNow();
    }

    // End-to-end: the workflow started by the winning node must actually run to completion via
    // whichever node's worker (both are live here) picks up the shared-queue activity task.
    awaitJobGone(managementServiceA, jobId);
  }

  @Test
  void dispatchingNodeWithNoLiveWorkerDoesNotOrphanTheJob() throws Exception {
    String dbName = "orphan-recovery-" + UUID.randomUUID();
    String taskQueue = "flow-job-executor-test-" + UUID.randomUUID();
    WorkflowClient workflowClient = newWorkflowClient();

    executorA = new ClusterSafeJobExecutor(workflowClient, taskQueue, Long.MAX_VALUE / 2);
    executorB = new ClusterSafeJobExecutor(workflowClient, taskQueue, Long.MAX_VALUE / 2);
    engineA = buildEngine(dbName, "node-A-" + dbName, executorA);
    engineB = buildEngine(dbName, "node-B-" + dbName, executorB);

    engineA.getRepositoryService().createDeployment().addString("asyncJobProcess.bpmn", PROCESS_XML).deploy();

    ProcessInstance instance = engineA.getRuntimeService().startProcessInstanceByKey("asyncJobProcess");
    ManagementService managementServiceA = engineA.getManagementService();
    Job job = managementServiceA.createJobQuery().processInstanceId(instance.getId()).singleResult();
    assertThat(job).isNotNull();
    String jobId = job.getId();

    // Node B is the only node whose Temporal worker ever comes up - it is the only live worker on
    // the shared queue.
    executorB.start();

    // Node A claims and dispatches the job (its own AcquireJobsCmd call succeeds, and it can talk
    // to the Temporal frontend to start the workflow) but deliberately never starts its own
    // worker factory at all here, simulating a node that crashes immediately after dispatching -
    // before it ever gets a chance to execute anything itself. triggerAcquisitionCycle() does not
    // require the executor to be started.
    List<String> claimedByA = executorA.triggerAcquisitionCycle();
    assertThat(claimedByA).containsExactly(jobId);

    // The job must still complete - executed by node B's worker on the shared queue - even though
    // the node that dispatched it never ran a worker of its own. This is the exact failure mode
    // §10 risk 12 describes ("a lost job"): proving it is NOT lost here is the point of this test.
    awaitJobGone(managementServiceA, jobId);
  }

  private static ProcessEngine buildEngine(String dbName, String engineName, ClusterSafeJobExecutor jobExecutor) {
    StandaloneInMemProcessEngineConfiguration config = new StandaloneInMemProcessEngineConfiguration();
    config.setProcessEngineName(engineName);
    config.setJdbcUrl("jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000");
    config.setDatabaseSchemaUpdate(ProcessEngineConfiguration.DB_SCHEMA_UPDATE_TRUE);
    config.setJobExecutorActivate(false);
    config.setJobExecutor(jobExecutor);
    config.setBpmnStacktraceVerbose(false);
    config.setDbMetricsReporterActivate(false);
    config.setTaskMetricsEnabled(false);
    config.setEnforceHistoryTimeToLive(false);
    return config.buildProcessEngine();
  }

  private WorkflowClient newWorkflowClient() {
    temporalServiceStubs = WorkflowServiceStubs.newServiceStubs(
        WorkflowServiceStubsOptions.newBuilder().setTarget("127.0.0.1:7233").build());
    return WorkflowClient.newInstance(temporalServiceStubs,
        WorkflowClientOptions.newBuilder().setNamespace("default").build());
  }

  private static void awaitJobGone(ManagementService managementService, String jobId) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 20_000;
    while (System.currentTimeMillis() < deadline) {
      if (managementService.createJobQuery().jobId(jobId).singleResult() == null) {
        return;
      }
      Thread.sleep(200);
    }
    throw new AssertionError("job " + jobId + " was not executed to completion within the timeout");
  }

}
