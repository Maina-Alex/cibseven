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
package org.cibseven.bpm.flow.tools;

import java.util.Map;

import org.cibseven.bpm.engine.ProcessEngineException;
import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.Expression;
import org.cibseven.bpm.engine.delegate.JavaDelegate;

/**
 * Wires a plain Service Task to a {@link Tool} resolved by path from {@link ToolRegistry}.
 * Configured via field injection:
 *
 * <pre>{@code
 * <serviceTask camunda:class="org.cibseven.bpm.flow.tools.ToolDelegate">
 *   <extensionElements>
 *     <camunda:field name="toolPath"><camunda:string>math/add</camunda:string></camunda:field>
 *     <camunda:field name="resultVariable"><camunda:string>toolResult</camunda:string></camunda:field>
 *   </extensionElements>
 * </serviceTask>
 * }</pre>
 *
 * The tool receives every current process variable as its input map and its return value is
 * written back to {@code resultVariable} (default: {@code toolResult}).
 */
public class ToolDelegate implements JavaDelegate {

  private Expression toolPath;
  private Expression resultVariable;

  @Override
  public void execute(DelegateExecution execution) throws Exception {
    String path = toolPath.getValue(execution).toString();
    Tool tool = ToolRegistry.getDefault().resolve(path)
        .orElseThrow(() -> new ProcessEngineException("No tool registered at path '" + path + "'"));

    Map<String, Object> input = execution.getVariables();
    Object result = tool.invoke(input);

    String variableName = resultVariable != null ? resultVariable.getValue(execution).toString() : "toolResult";
    execution.setVariable(variableName, result);
  }

  public void setToolPath(Expression toolPath) {
    this.toolPath = toolPath;
  }

  public void setResultVariable(Expression resultVariable) {
    this.resultVariable = resultVariable;
  }

}
