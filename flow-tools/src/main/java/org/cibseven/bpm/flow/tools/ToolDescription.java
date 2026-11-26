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

/**
 * Describes a tool registered in a {@link ToolRegistry}: its stable lookup path, a
 * human/LLM-readable description, and its input/output schema (JSON Schema text), the same
 * shape an AI Task uses for tool selection.
 */
public record ToolDescription(String path, String description, String inputSchema, String outputSchema) {

  public ToolDescription {
    if (path == null || path.isBlank()) {
      throw new IllegalArgumentException("Tool path must not be blank");
    }
  }

}
