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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.cibseven.bpm.spring.boot.starter.flowjobexecutor.test.TestFlowJobExecutorApplication;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * Enabling cluster mode together with {@code camunda.bpm.id-generator=simple} - which registers
 * no Spring {@code IdGenerator} bean at all and falls through to the engine's own built-in,
 * collision-prone {@code DbIdGenerator} default (confirmed against the base starter's own
 * {@code SimpleUuidGeneratorIT}) - must fail application startup loudly via
 * {@link FlowJobExecutorClusterSafetyGuard}, rather than silently booting into the exact
 * misconfiguration that reopened §10 risk 12. Deliberately boots via {@link SpringApplicationBuilder}
 * directly (instead of {@code @SpringBootTest} on the test class) so the startup failure itself -
 * not a JUnit extension's own wrapping of it - is what this test asserts on.
 */
class FlowJobExecutorUnsafeIdGeneratorFailsFastIT {

  @Test
  void clusterModeWithUnsafeIdGeneratorFailsApplicationStartupLoudly() {
    assertThatThrownBy(() ->
        new SpringApplicationBuilder(TestFlowJobExecutorApplication.class)
            .web(WebApplicationType.NONE)
            .properties(
                "camunda.bpm.process-engine-name=unsafeIdGeneratorEngine",
                "camunda.bpm.database.schema-update=true",
                "camunda.bpm.id-generator=simple",
                "cibseven.bpm.flow.job-executor.enabled=true",
                "cibseven.bpm.flow.job-executor.temporal-target=127.0.0.1:17233")
            .run())
        .satisfies(startupFailure -> assertThat(rootCauseChainContains(startupFailure,
            UnsafeIdGeneratorForClusterModeException.class)).isTrue())
        .satisfies(startupFailure -> assertThat(messageAnywhereInCauseChain(startupFailure))
            .contains("requires a collision-resistant IdGenerator"));
  }

  private static boolean rootCauseChainContains(Throwable throwable, Class<?> type) {
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      if (type.isInstance(t)) {
        return true;
      }
    }
    return false;
  }

  private static String messageAnywhereInCauseChain(Throwable throwable) {
    StringBuilder all = new StringBuilder();
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      if (t.getMessage() != null) {
        all.append(t.getMessage()).append('\n');
      }
    }
    return all.toString();
  }

}
