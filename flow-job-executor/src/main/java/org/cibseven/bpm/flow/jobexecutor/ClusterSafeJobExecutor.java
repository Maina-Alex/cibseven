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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.cibseven.bpm.engine.impl.ProcessEngineImpl;
import org.cibseven.bpm.engine.impl.cmd.AcquireJobsCmd;
import org.cibseven.bpm.engine.impl.interceptor.CommandExecutor;
import org.cibseven.bpm.engine.impl.jobexecutor.AcquiredJobs;
import org.cibseven.bpm.engine.impl.jobexecutor.ExecuteJobHelper;
import org.cibseven.bpm.engine.impl.jobexecutor.JobExecutor;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.client.WorkflowOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replaces CIB7's default polling {@link JobExecutor} with one driven by Temporal, closing the
 * multi-node gap in docs/cib7-fork-engine-plan.md §6 / §10 risk 12: every job dispatch is preceded
 * by a real, atomic claim (reusing {@link AcquireJobsCmd}'s own lock-owner/lock-expiration
 * optimistic-locking columns on {@code ACT_RU_JOB} - the same mechanism CIB7's own
 * {@code AcquireJobsRunnable} uses), and every claimed job is dispatched to a Temporal Workflow on
 * one shared task queue name rather than a private per-node queue, so a worker on any live node -
 * not only the one that claimed the job - can execute it.
 *
 * <p>This closes both halves of the gap: (1) two nodes racing the same due job can no longer both
 * believe they own it - the loser's {@code AcquireJobsCmd} optimistic-locking failure silently
 * drops it from that node's acquired batch; (2) a node that crashes after claiming (or after
 * starting the Temporal workflow but before its own worker executes it) no longer orphans the job
 * - either a surviving node's worker on the shared queue picks up the already-started workflow's
 * activity task, or, if the crash happened before the Temporal dispatch call itself ever
 * succeeded, the job's lock expires and the periodic sweep (see {@link #startSweep()}) reclaims
 * and redispatches it, same as a brand-new job.
 *
 * <p>Deliberately does not start CIB7's own {@code AcquireJobsRunnable} polling thread
 * ({@link #startExecutingJobs()} never calls {@code startJobAcquisitionThread()}) - acquisition is
 * instead driven by the {@link #jobWasAdded()} event plus this class's own lightweight periodic
 * sweep, both of which route every claim through the real {@link AcquireJobsCmd}.
 */
public class ClusterSafeJobExecutor extends JobExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterSafeJobExecutor.class);

  /**
   * One task queue name shared by every engine node in the cluster. Must be identical across all
   * {@code ClusterSafeJobExecutor} instances that are meant to cooperate - a per-node queue name
   * is exactly the bug this class exists to avoid. Enforced structurally, not just by convention:
   * {@link #startExecutingJobs()} refuses to start a node whose value disagrees with what this
   * cluster's shared engine database already recorded (see {@link RegisterOrVerifySharedTaskQueueCmd}).
   */
  private final String sharedTaskQueueName;
  private final WorkflowClient workflowClient;
  private final long sweepIntervalMillis;

  private WorkerFactory workerFactory;
  private ScheduledExecutorService sweepExecutor;

  public ClusterSafeJobExecutor(WorkflowClient workflowClient, String sharedTaskQueueName) {
    this(workflowClient, sharedTaskQueueName, 2000L);
  }

  public ClusterSafeJobExecutor(WorkflowClient workflowClient, String sharedTaskQueueName, long sweepIntervalMillis) {
    this.workflowClient = workflowClient;
    this.sharedTaskQueueName = sharedTaskQueueName;
    this.sweepIntervalMillis = sweepIntervalMillis;
  }

  @Override
  protected void startExecutingJobs() {
    ProcessEngineImpl engine = getProcessEngines().get(0);
    CommandExecutor commandExecutor = engine.getProcessEngineConfiguration().getCommandExecutorTxRequired();

    // Structural guard against §10 risk 12's exact misconfiguration: refuse to start rather
    // than run with a queue name that disagrees with what this cluster's shared database has
    // already recorded. Must run before the worker factory comes up - a node that fails this
    // check must never claim or dispatch a single job.
    commandExecutor.execute(new RegisterOrVerifySharedTaskQueueCmd(sharedTaskQueueName));

    workerFactory = WorkerFactory.newInstance(workflowClient);
    Worker worker = workerFactory.newWorker(sharedTaskQueueName);
    worker.registerWorkflowImplementationTypes(JobDispatchWorkflowImpl.class);
    worker.registerActivitiesImplementations(
        new JobExecutionActivitiesImpl(commandExecutor, engine.getManagementService()));
    workerFactory.start();

    startSweep();
  }

  @Override
  protected void stopExecutingJobs() {
    if (sweepExecutor != null) {
      sweepExecutor.shutdownNow();
      sweepExecutor = null;
    }
    if (workerFactory != null) {
      workerFactory.shutdown();
      workerFactory = null;
    }
  }

  @Override
  public void executeJobs(List<String> jobIds, ProcessEngineImpl processEngine) {
    // Direct synchronous fallback - not this executor's primary path (real dispatch always goes
    // through Temporal via jobWasAdded()/the sweep), but required by the abstract contract and
    // useful for a caller that already holds a claimed batch and wants it run inline.
    CommandExecutor commandExecutor = processEngine.getProcessEngineConfiguration().getCommandExecutorTxRequired();
    for (String jobId : jobIds) {
      ExecuteJobHelper.executeJob(jobId, commandExecutor);
    }
  }

  @Override
  public void jobWasAdded() {
    if (isActive()) {
      acquireAndDispatch();
    }
  }

  /**
   * Runs one acquisition+dispatch cycle synchronously and returns the job ids this node's
   * {@link AcquireJobsCmd} call actually claimed (as opposed to lost the optimistic-locking race
   * for). Exposed for tests that need deterministic, on-demand control over acquisition timing
   * instead of waiting on the event-driven hook or the periodic sweep.
   */
  public List<String> triggerAcquisitionCycle() {
    return acquireAndDispatch();
  }

  /**
   * Test-only hook simulating an abrupt crash of this node's live worker - as opposed to the
   * graceful {@link #shutdown()}, which this deliberately does not call. Uses
   * {@link WorkerFactory#shutdownNow()} to forcibly interrupt whatever activity task that worker
   * is currently executing, rather than letting it run to completion, so a test can prove recovery
   * for a node that dies <em>while</em> executing a job - the case
   * {@code dispatchingNodeWithNoLiveWorkerDoesNotOrphanTheJob} does not cover, since there a node
   * never starts a worker at all.
   */
  public void simulateCrashOfLiveWorker() {
    if (sweepExecutor != null) {
      sweepExecutor.shutdownNow();
      sweepExecutor = null;
    }
    if (workerFactory != null) {
      workerFactory.shutdownNow();
      workerFactory = null;
    }
  }

  private void startSweep() {
    sweepExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "cluster-safe-job-executor-sweep");
      t.setDaemon(true);
      return t;
    });
    sweepExecutor.scheduleWithFixedDelay(this::safeSweep, sweepIntervalMillis, sweepIntervalMillis, TimeUnit.MILLISECONDS);
  }

  private void safeSweep() {
    try {
      if (isActive()) {
        acquireAndDispatch();
      }
    } catch (RuntimeException e) {
      // A single failed sweep tick must never kill the scheduled task; the next tick retries.
      LOG.warn("Cluster-safe job executor sweep tick failed, will retry on the next tick", e);
    }
  }

  /**
   * The one choke point every acquisition path (event-driven and swept) goes through: claim via
   * the real {@link AcquireJobsCmd}, dispatch to Temporal only the jobs this node's claim actually
   * won.
   */
  private List<String> acquireAndDispatch() {
    if (getProcessEngines().isEmpty()) {
      return List.of();
    }
    ProcessEngineImpl engine = getProcessEngines().get(0);
    CommandExecutor commandExecutor = engine.getProcessEngineConfiguration().getCommandExecutorTxRequired();

    AcquiredJobs acquiredJobs = commandExecutor.execute(new AcquireJobsCmd(this, getMaxJobsPerAcquisition()));

    List<String> claimedJobIds = new ArrayList<>();
    for (List<String> batch : acquiredJobs.getJobIdBatches()) {
      claimedJobIds.addAll(batch);
    }

    for (String jobId : claimedJobIds) {
      dispatchToTemporal(jobId);
    }
    return claimedJobIds;
  }

  private void dispatchToTemporal(String jobId) {
    JobDispatchWorkflow workflow = workflowClient.newWorkflowStub(
        JobDispatchWorkflow.class,
        WorkflowOptions.newBuilder()
            .setTaskQueue(sharedTaskQueueName)
            .setWorkflowId(workflowIdFor(jobId))
            .build());
    try {
      WorkflowClient.start(workflow::runJob, jobId);
    } catch (WorkflowExecutionAlreadyStarted e) {
      // A workflow for this job id is already running (started by this node on a previous
      // acquisition tick, or - defense in depth, should not happen given the AcquireJobsCmd claim
      // above - by another node). Not an error: exactly the dedupe this design relies on.
    }
  }

  private static String workflowIdFor(String jobId) {
    return "flow-job:" + jobId;
  }

}
