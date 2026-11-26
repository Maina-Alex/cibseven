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
package org.cibseven.bpm.flow.audit;

import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.JavaDelegate;

/**
 * Sets a variable the same way {@code ToolDelegate}/{@code AiTaskBehavior} do: directly via
 * {@code DelegateExecution.setVariable(...)} mid-transaction, with no dedicated top-level
 * Command issued. Used to prove that path does NOT get an attributable
 * {@code UserOperationLogEntry}, unlike a public-API call such as
 * {@code RuntimeService.setVariable(...)}.
 */
public class DirectVariableSetDelegate implements JavaDelegate {

  @Override
  public void execute(DelegateExecution execution) {
    execution.setVariable("viaDelegate", "value-b");
  }

}
