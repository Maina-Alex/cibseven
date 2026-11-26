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

import java.util.Map;

/** Test double: confidence comes straight from a process variable so both gate paths are forceable. */
public class FakeAiTaskExecutor implements AiTaskExecutor {

  @Override
  public AiTaskResult execute(AiTaskConfig config, Map<String, Object> processVariables) {
    double confidence = ((Number) processVariables.get("forcedConfidence")).doubleValue();
    return new AiTaskResult("ai-generated-output", confidence, "approve");
  }

}
