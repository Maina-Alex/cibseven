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

import org.cibseven.bpm.engine.impl.jobexecutor.JobExecutor;
import org.cibseven.bpm.engine.spring.components.jobexecutor.SpringJobExecutor;
import org.cibseven.bpm.flow.jobexecutor.ClusterSafeJobExecutor;
import org.cibseven.bpm.spring.boot.starter.flowjobexecutor.test.TestFlowJobExecutorApplication;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * With this module's own property left at its default ({@code false}), this starter-flow-job-
 * executor module must be a complete no-op: the base starter's own {@code SpringJobExecutor} bean
 * (see {@code DefaultJobConfiguration.JobConfiguration#jobExecutor}) wins, completely unchanged -
 * proving this module is additive, not a regression on the existing default path.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(
    classes = { TestFlowJobExecutorApplication.class },
    webEnvironment = WebEnvironment.NONE,
    properties = {
        "camunda.bpm.process-engine-name=clusterModeDisabledEngine",
        "camunda.bpm.database.schema-update=true"
        // cibseven.bpm.flow.job-executor.enabled left at its default (false) deliberately.
    })
class FlowJobExecutorDisabledByDefaultIT {

  @Autowired
  private JobExecutor jobExecutor;

  @Test
  void defaultBehaviorIsCompletelyUnchangedWhenClusterModeIsNotEnabled() {
    assertThat(jobExecutor).isInstanceOf(SpringJobExecutor.class);
    assertThat(jobExecutor).isNotInstanceOf(ClusterSafeJobExecutor.class);
  }

}
