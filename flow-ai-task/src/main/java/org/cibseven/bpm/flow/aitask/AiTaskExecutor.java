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

/**
 * Runs the LLM for one AI Task invocation. {@link AiTaskBehavior} depends only on this
 * interface, not on a particular LLM integration — a CIB7 AI Agent Connector /
 * LangChain4j-backed implementation (reaching into {@code flow-tools}' {@code ToolRegistry}
 * for {@link AiTaskConfig#toolPaths()}) plugs in here without the gate/handover logic in
 * {@link AiTaskBehavior} changing.
 */
public interface AiTaskExecutor {

  AiTaskResult execute(AiTaskConfig config, Map<String, Object> processVariables);

}
