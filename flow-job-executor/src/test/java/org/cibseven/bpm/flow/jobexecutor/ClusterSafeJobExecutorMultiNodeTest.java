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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
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
import org.cibseven.bpm.engine.impl.ProcessEngineImpl;
import org.cibseven.bpm.engine.impl.cfg.StandaloneInMemProcessEngineConfiguration;
import org.cibseven.bpm.engine.impl.interceptor.CommandExecutor;
import org.cibseven.bpm.engine.runtime.Job;
import org.cibseven.bpm.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.WorkflowOptions;
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

  private static final String BLOCKING_PROCESS_XML = """
      <?xml version="1.0" encoding="UTF-8"?>
      <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
                   xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                   id="definitions"
                   targetNamespace="http://cibseven.org/flow/jobexecutor/test">
        <process id="blockingAsyncJobProcess" isExecutable="true">
          <startEvent id="start" />
          <sequenceFlow id="f1" sourceRef="start" targetRef="asyncTask" />
          <serviceTask id="asyncTask" camunda:asyncBefore="true"
                       camunda:class="org.cibseven.bpm.flow.jobexecutor.BlockingThenFastServiceDelegate" />
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

  @Test
  void refusesToStartWhenConfiguredWithADifferentQueueNameThanTheRestOfTheCluster() throws Exception {
    String dbName = "queue-guard-" + UUID.randomUUID();
    WorkflowClient workflowClient = newWorkflowClient();

    // Node A starts first and, in doing so, registers "queue-a" as this cluster's canonical
    // shared queue name (RegisterOrVerifySharedTaskQueueCmd, first writer wins).
    executorA = new ClusterSafeJobExecutor(workflowClient, "queue-a-" + dbName, Long.MAX_VALUE / 2);
    engineA = buildEngine(dbName, "node-A-" + dbName, executorA);
    executorA.start();

    // Node B is misconfigured with a different queue name than node A already registered -
    // exactly the misconfiguration docs/cib7-fork-engine-plan.md §10 risk 12 warns would silently
    // reintroduce the lost-job bug if nothing caught it. It must refuse to start.
    executorB = new ClusterSafeJobExecutor(workflowClient, "queue-b-" + dbName, Long.MAX_VALUE / 2);
    engineB = buildEngine(dbName, "node-B-" + dbName, executorB);

    assertThatThrownBy(executorB::start)
        .as("a node configured with a shared task queue name that disagrees with what this "
            + "cluster's engine database already recorded must refuse to start")
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("queue-a-" + dbName)
        .hasMessageContaining("queue-b-" + dbName);

    // Confirm the refusal was real, not cosmetic: node B never came up as a worker at all, so a
    // job dispatched to node A's queue has only node A's worker to run it.
    engineA.getRepositoryService().createDeployment().addString("asyncJobProcess.bpmn", PROCESS_XML).deploy();
    ProcessInstance instance = engineA.getRuntimeService().startProcessInstanceByKey("asyncJobProcess");
    ManagementService managementServiceA = engineA.getManagementService();
    Job job = managementServiceA.createJobQuery().processInstanceId(instance.getId()).singleResult();
    assertThat(job).isNotNull();
    executorA.triggerAcquisitionCycle();
    awaitJobGone(managementServiceA, job.getId());
  }

  @Test
  void aNodeThatCrashesWhileAWorkerIsMidActivityStillGetsTheJobCompletedByASurvivor() throws Exception {
    BlockingThenFastServiceDelegate.reset();
    String dbName = "mid-activity-crash-" + UUID.randomUUID();
    String taskQueue = "flow-job-executor-test-" + UUID.randomUUID();
    WorkflowClient workflowClient = newWorkflowClient();

    executorA = new ClusterSafeJobExecutor(workflowClient, taskQueue, Long.MAX_VALUE / 2);
    executorB = new ClusterSafeJobExecutor(workflowClient, taskQueue, Long.MAX_VALUE / 2);
    engineA = buildEngine(dbName, "node-A-" + dbName, executorA);
    engineB = buildEngine(dbName, "node-B-" + dbName, executorB);

    engineA.getRepositoryService().createDeployment().addString("blockingAsyncJobProcess.bpmn", BLOCKING_PROCESS_XML).deploy();
    ProcessInstance instance = engineA.getRuntimeService().startProcessInstanceByKey("blockingAsyncJobProcess");
    ManagementService managementServiceA = engineA.getManagementService();
    Job job = managementServiceA.createJobQuery().processInstanceId(instance.getId()).singleResult();
    assertThat(job).isNotNull();
    String jobId = job.getId();

    // Node A alone claims and dispatches, then its own live worker genuinely starts executing
    // the activity - this is the case dispatchingNodeWithNoLiveWorkerDoesNotOrphanTheJob does not
    // cover (there, node A never starts a worker at all).
    executorA.start();
    List<String> claimedByA = executorA.triggerAcquisitionCycle();
    assertThat(claimedByA).containsExactly(jobId);

    boolean genuinelyStarted = BlockingThenFastServiceDelegate.awaitFirstInvocationStarted(30);
    assertThat(genuinelyStarted)
        .as("the activity must be observably mid-execution on node A before node A is crashed, "
            + "or this test would not actually be proving recovery from an in-flight crash")
        .isTrue();

    long crashedAtMillis = System.currentTimeMillis();
    // The crash: forcibly interrupt node A's worker mid-activity, instead of a graceful shutdown
    // that would wait for the (120s-sleeping) activity to finish on its own.
    executorA.simulateCrashOfLiveWorker();

    // A surviving node comes up on the same shared queue - the only way this job can now be
    // completed, since node A's worker is gone for good and never restarts in this test.
    executorB.start();

    awaitJobGone(managementServiceA, jobId, 60_000);
    long recoveredAfterMillis = System.currentTimeMillis() - crashedAtMillis;
    System.out.println("[mid-activity-crash test] job recovered " + recoveredAfterMillis
        + "ms after the crashed node's worker was killed (StartToCloseTimeout=30s, no heartbeat "
        + "timeout configured, so Temporal's own timeout is expected to dominate this duration)");

    assertThat(BlockingThenFastServiceDelegate.invocationCount())
        .as("the delegate must have been re-invoked by the retry (attempt 1 was the one that got "
            + "killed mid-sleep; completion requires a second, distinct invocation)")
        .isGreaterThanOrEqualTo(2);
  }

  @Test
  void twoDifferentSchemaIncarnationsWithTheSameSmallJobIdDoNotCollideOnTheSharedTemporalServer() throws Exception {
    // Reproduces the exact scenario that used to lose a job: two *different* engine schemas
    // (simulating a dev/staging reset, a DR restore, or simply re-running this suite against an
    // already-used long-lived Temporal server) each producing their own job id starting from a
    // fresh id-generator watermark - so both commonly land on the same small integer - dispatched
    // to the *same* Temporal server on the *same* task queue. Before the installation-fingerprint
    // fix, the second job's dispatch would silently collide with the first job's already-completed
    // workflow under the same derived id and never run; afterwards, both must complete normally.
    String taskQueue = "flow-job-executor-test-shared-" + UUID.randomUUID();
    WorkflowClient workflowClient = newWorkflowClient();

    String dbName1 = "incarnation-1-" + UUID.randomUUID();
    executorA = new ClusterSafeJobExecutor(workflowClient, taskQueue, Long.MAX_VALUE / 2);
    engineA = buildEngine(dbName1, "node-" + dbName1, executorA);
    engineA.getRepositoryService().createDeployment().addString("asyncJobProcess.bpmn", PROCESS_XML).deploy();
    ProcessInstance instance1 = engineA.getRuntimeService().startProcessInstanceByKey("asyncJobProcess");
    ManagementService managementService1 = engineA.getManagementService();
    Job job1 = managementService1.createJobQuery().processInstanceId(instance1.getId()).singleResult();
    assertThat(job1).isNotNull();
    String jobId1 = job1.getId();

    executorA.start();
    assertThat(executorA.triggerAcquisitionCycle()).containsExactly(jobId1);
    awaitJobGone(managementService1, jobId1);

    // First "incarnation" is done and torn down entirely - its engine, its executor, its worker.
    executorA.shutdown();
    engineA.close();

    // Second, independent engine - a genuinely fresh H2 schema, so its own id generator restarts
    // its watermark from scratch, dispatched to the exact same still-running Temporal server and
    // the exact same shared task queue name as the first.
    String dbName2 = "incarnation-2-" + UUID.randomUUID();
    executorB = new ClusterSafeJobExecutor(workflowClient, taskQueue, Long.MAX_VALUE / 2);
    engineB = buildEngine(dbName2, "node-" + dbName2, executorB);
    engineB.getRepositoryService().createDeployment().addString("asyncJobProcess.bpmn", PROCESS_XML).deploy();
    ProcessInstance instance2 = engineB.getRuntimeService().startProcessInstanceByKey("asyncJobProcess");
    ManagementService managementService2 = engineB.getManagementService();
    Job job2 = managementService2.createJobQuery().processInstanceId(instance2.getId()).singleResult();
    assertThat(job2).isNotNull();
    String jobId2 = job2.getId();

    System.out.println("[fingerprint test] incarnation 1 job id=" + jobId1 + ", incarnation 2 job id=" + jobId2
        + (jobId1.equals(jobId2) ? " (same raw id, as expected from two fresh id-generator watermarks)" : " (different raw ids this run)"));

    executorB.start();
    assertThat(executorB.triggerAcquisitionCycle()).containsExactly(jobId2);

    // The point of this test: this must complete normally even when jobId1 and jobId2 happen to
    // be the same string, because the two schemas' installation fingerprints differ.
    awaitJobGone(managementService2, jobId2);
  }

  @Test
  void aGenuineWorkflowIdCollisionIsDetectedAndFailsLoudlyInsteadOfSilentlySwallowingTheJob() throws Exception {
    String dbName = "deliberate-collision-" + UUID.randomUUID();
    String taskQueue = "flow-job-executor-test-" + UUID.randomUUID();
    WorkflowClient workflowClient = newWorkflowClient();

    executorA = new ClusterSafeJobExecutor(workflowClient, taskQueue, Long.MAX_VALUE / 2);
    engineA = buildEngine(dbName, "node-A-" + dbName, executorA);
    engineA.getRepositoryService().createDeployment().addString("asyncJobProcess.bpmn", PROCESS_XML).deploy();
    ProcessInstance instance = engineA.getRuntimeService().startProcessInstanceByKey("asyncJobProcess");
    ManagementService managementServiceA = engineA.getManagementService();
    Job job = managementServiceA.createJobQuery().processInstanceId(instance.getId()).singleResult();
    assertThat(job).isNotNull();
    String jobId = job.getId();

    // Deliberately reconstruct the exact workflow id a real dispatch of this job would derive,
    // then seed an unrelated stale workflow under that literal id - simulating a genuine
    // cross-incarnation collision directly, rather than waiting for one to occur naturally.
    CommandExecutor commandExecutor =
        ((ProcessEngineImpl) engineA).getProcessEngineConfiguration().getCommandExecutorTxRequired();
    String installationFingerprint = commandExecutor.execute(new GetOrCreateInstallationFingerprintCmd());
    String workflowId = ClusterSafeJobExecutor.workflowIdFor(installationFingerprint, jobId);

    String unrelatedJobId = "unrelated-stale-job-" + UUID.randomUUID();
    JobDispatchWorkflow staleWorkflow = workflowClient.newWorkflowStub(
        JobDispatchWorkflow.class,
        WorkflowOptions.newBuilder()
            .setTaskQueue(taskQueue)
            .setWorkflowId(workflowId)
            .setMemo(Map.of(ClusterSafeJobExecutor.JOB_ID_MEMO_KEY, unrelatedJobId))
            .build());
    WorkflowClient.start(staleWorkflow::runJob, unrelatedJobId);

    // Now dispatch the real job. No worker is started for taskQueue at all here - the point is
    // purely what happens at dispatch time, not whether anything ever executes the stale
    // workflow's activity.
    assertThatThrownBy(executorA::triggerAcquisitionCycle)
        .as("a workflow-id collision with an unrelated job's memo must fail loudly, never be "
            + "silently swallowed as if it were this job's own retry")
        .isInstanceOf(JobDispatchCollisionException.class)
        .hasMessageContaining(jobId)
        .hasMessageContaining(unrelatedJobId);

    // Failing loudly, not silently: the real job must still be sitting there, claimed but not
    // executed - exactly the "not lost, surfaced instead" outcome this fix is meant to produce.
    assertThat(managementServiceA.createJobQuery().jobId(jobId).singleResult())
        .as("the real job must not have silently vanished - it should still exist, awaiting a "
            + "retry once the collision is resolved")
        .isNotNull();
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
    awaitJobGone(managementService, jobId, 20_000);
  }

  private static void awaitJobGone(ManagementService managementService, String jobId, long timeoutMillis) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (System.currentTimeMillis() < deadline) {
      if (managementService.createJobQuery().jobId(jobId).singleResult() == null) {
        return;
      }
      Thread.sleep(200);
    }
    throw new AssertionError("job " + jobId + " was not executed to completion within the timeout");
  }

}
