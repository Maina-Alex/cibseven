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

import org.cibseven.bpm.engine.ProcessEngine;
import org.cibseven.bpm.engine.impl.jobexecutor.JobExecutor;
import org.cibseven.bpm.flow.jobexecutor.ClusterSafeJobExecutor;
import org.cibseven.bpm.spring.boot.starter.CamundaBpmAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;

/**
 * Opt-in auto-configuration swapping CIB7's default polling {@link JobExecutor} for
 * {@link ClusterSafeJobExecutor} (see docs/cib7-fork-engine-plan.md §6/§10 risk 12) when
 * {@code cibseven.bpm.flow.job-executor.enabled=true}. Additive and off by default: when disabled,
 * nothing in this module is active and the base starter's own {@code JobExecutor} bean (see
 * {@code org.cibseven.bpm.spring.boot.starter.configuration.impl.DefaultJobConfiguration}) wins,
 * completely unchanged.
 *
 * <p>Declared {@code @AutoConfigureBefore(CamundaBpmAutoConfiguration.class)} so this class's own
 * {@code JobExecutor} bean (when cluster mode is enabled) is registered before
 * {@code CamundaBpmAutoConfiguration}'s imported {@code DefaultJobConfiguration.JobConfiguration}
 * evaluates its own {@code @ConditionalOnMissingBean(JobExecutor.class)} - that ordering is what
 * lets this one win without touching the base starter at all.
 */
@Configuration
@EnableConfigurationProperties(FlowJobExecutorProperties.class)
@ConditionalOnProperty(prefix = FlowJobExecutorProperties.PREFIX, name = "enabled", havingValue = "true")
@AutoConfigureBefore(CamundaBpmAutoConfiguration.class)
public class FlowJobExecutorAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(JobExecutor.class)
  public JobExecutor jobExecutor(FlowJobExecutorProperties properties) {
    WorkflowServiceStubs serviceStubs = WorkflowServiceStubs.newServiceStubs(
        WorkflowServiceStubsOptions.newBuilder()
            .setTarget(properties.getTemporalTarget())
            .build());
    WorkflowClient workflowClient = WorkflowClient.newInstance(serviceStubs,
        WorkflowClientOptions.newBuilder()
            .setNamespace(properties.getTemporalNamespace())
            .build());
    return new ClusterSafeJobExecutor(workflowClient, properties.getTaskQueue(), properties.getSweepIntervalMillis());
  }

  @Bean
  public FlowJobExecutorClusterSafetyGuard flowJobExecutorClusterSafetyGuard(ProcessEngine processEngine) {
    return new FlowJobExecutorClusterSafetyGuard(processEngine);
  }

}
