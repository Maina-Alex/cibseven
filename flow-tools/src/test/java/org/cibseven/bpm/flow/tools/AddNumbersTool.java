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

/** A minimal code-registered tool, used to prove {@link ToolDelegate} end to end. */
public class AddNumbersTool implements Tool {

  public static final ToolDescription DESCRIPTION = new ToolDescription(
      "math/add",
      "Adds two numbers, 'a' and 'b', and returns their sum as 'sum'.",
      "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"number\"},\"b\":{\"type\":\"number\"}}}",
      "{\"type\":\"object\",\"properties\":{\"sum\":{\"type\":\"number\"}}}");

  @Override
  public ToolDescription getDescription() {
    return DESCRIPTION;
  }

  @Override
  public Object invoke(Map<String, Object> input) {
    Number a = (Number) input.get("a");
    Number b = (Number) input.get("b");
    return Map.of("sum", a.doubleValue() + b.doubleValue());
  }

}
