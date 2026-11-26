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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One registry, reached from two routes: code-registered tools call {@link #register(Tool)}
 * directly (e.g. at Spring context startup); a config-registered route (Flow's connector model,
 * ported natively — not yet implemented here) would populate the same registry from configured
 * resources/operations. {@link ToolDelegate} and the AI Task's tool-calling loop both resolve
 * against this one instance, which is why it exposes a {@linkplain #getDefault() static default}:
 * {@link ToolDelegate} is wired via {@code camunda:class} and instantiated by unmanaged
 * reflection per execution, so it has no DI container to pull a registry from.
 */
public class ToolRegistry {

  private static final ToolRegistry DEFAULT = new ToolRegistry();

  public static ToolRegistry getDefault() {
    return DEFAULT;
  }

  private final Map<String, Tool> toolsByPath = new ConcurrentHashMap<>();

  public void register(Tool tool) {
    String path = tool.getDescription().path();
    toolsByPath.put(path, tool);
  }

  public void unregister(String path) {
    toolsByPath.remove(path);
  }

  public Optional<Tool> resolve(String path) {
    return Optional.ofNullable(toolsByPath.get(path));
  }

  public Collection<ToolDescription> listDescriptions() {
    return toolsByPath.values().stream().map(Tool::getDescription).toList();
  }

  /** Test-only: clears every registered tool so tests don't leak state across cases. */
  public void clear() {
    toolsByPath.clear();
  }

}
