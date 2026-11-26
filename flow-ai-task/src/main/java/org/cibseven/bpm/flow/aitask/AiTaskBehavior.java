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

import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.JavaDelegate;

/**
 * Wires the AI Task archetype to a Service Task. Runs the configured {@link AiTaskExecutor}
 * against this activity's {@link AiTaskConfig}, then sets process variables an adjoining
 * exclusive gateway routes on: {@code aiConfidenceMet} selects auto-resolve vs. handover,
 * {@code recommendedDisposition}/{@code handoverQueue} carry what the handover branch's
 * Human Task needs.
 *
 * <p>Wired via {@code camunda:class}, so it is instantiated by unmanaged reflection per
 * execution with no DI container available; {@link #setExecutor(AiTaskExecutor)} is the
 * static-singleton-accessor wiring point application startup code uses to plug in a real
 * executor (same pattern as {@code ToolRegistry.getDefault()}).
 */
public class AiTaskBehavior implements JavaDelegate {

  private static volatile AiTaskExecutor executor = (config, variables) -> {
    throw new IllegalStateException(
        "No AiTaskExecutor configured. Call AiTaskBehavior.setExecutor(...) at startup.");
  };

  public static void setExecutor(AiTaskExecutor newExecutor) {
    executor = newExecutor;
  }

  @Override
  public void execute(DelegateExecution execution) {
    AiTaskConfig config = AiTaskConfigReader.readFrom(execution);
    AiTaskResult result = executor.execute(config, execution.getVariables());

    boolean confidenceMet = result.confidence() >= config.confidenceThreshold();

    execution.setVariable("aiOutput", result.output());
    execution.setVariable("aiConfidence", result.confidence());
    execution.setVariable("aiConfidenceMet", confidenceMet);
    execution.setVariable("recommendedDisposition", result.recommendedDisposition());
    if (!confidenceMet) {
      execution.setVariable("handoverQueue", config.handoverQueue());
    }
  }

}
