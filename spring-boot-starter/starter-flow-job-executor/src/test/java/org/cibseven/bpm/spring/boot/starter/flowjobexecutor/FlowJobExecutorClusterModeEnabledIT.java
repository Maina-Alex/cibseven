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
package org.cibseven.bpm.spring.boot.starter.flowjobexecutor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.util.ReflectionTestUtils.getField;

import org.cibseven.bpm.engine.impl.jobexecutor.JobExecutor;
import org.cibseven.bpm.flow.jobexecutor.ClusterSafeJobExecutor;
import org.cibseven.bpm.spring.boot.starter.flowjobexecutor.test.TestFlowJobExecutorApplication;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Enabling cluster mode genuinely swaps in {@link ClusterSafeJobExecutor} as the real
 * {@code JobExecutor} bean, wired with the externalized Temporal properties. Turns out a live
 * Temporal server IS required for this, contrary to the original assumption: this starter's
 * {@code ProcessApplicationEventPublisher} republishes {@code ApplicationReadyEvent} as a
 * {@code ProcessApplicationStartedEvent} unconditionally (no registered process application
 * needed), so {@code JobExecutorStartingEventListener} genuinely calls {@code jobExecutor.start()}
 * during context startup, and {@code WorkerFactory.start()} synchronously calls
 * {@code describeNamespace} against the configured target - requires a reachable Temporal server
 * on {@code 127.0.0.1:7233} (the default namespace), not a stub target.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(
    classes = { TestFlowJobExecutorApplication.class },
    webEnvironment = WebEnvironment.NONE,
    properties = {
        "camunda.bpm.process-engine-name=clusterModeEnabledEngine",
        "camunda.bpm.database.schema-update=true",
        "cibseven.bpm.flow.job-executor.enabled=true",
        "cibseven.bpm.flow.job-executor.temporal-target=127.0.0.1:7233",
        "cibseven.bpm.flow.job-executor.temporal-namespace=default",
        "cibseven.bpm.flow.job-executor.task-queue=flow-test-task-queue"
    })
class FlowJobExecutorClusterModeEnabledIT {

  @Autowired
  private JobExecutor jobExecutor;

  @Test
  void clusterModeSwapsInClusterSafeJobExecutorConfiguredFromProperties() {
    assertThat(jobExecutor).isInstanceOf(ClusterSafeJobExecutor.class);
    assertThat(getField(jobExecutor, "sharedTaskQueueName")).isEqualTo("flow-test-task-queue");
    assertThat(getField(jobExecutor, "workflowClient")).isNotNull();
  }

}
