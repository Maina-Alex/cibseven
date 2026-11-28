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

import org.cibseven.bpm.engine.impl.cfg.IdGenerator;

/**
 * Thrown at application startup when {@link FlowJobExecutorProperties#isEnabled()} is {@code true}
 * but the resolved {@link IdGenerator} is not safe for clustering. {@code flow-job-executor}'s
 * workflow-id collision fix (see docs/cib7-fork-engine-plan.md §6/§10 risk 12) depends on job ids
 * never repeating across a schema reset in a way that collides with a stale Temporal workflow
 * execution; CIB7's default {@code DbIdGenerator} is a plain incrementing sequence that reseeds to
 * the same small integers on any freshly created schema, defeating that assumption. Set
 * {@code camunda.bpm.id-generator=strong} (the engine's own default) or {@code =prefixed}, or
 * disable cluster mode.
 */
public class UnsafeIdGeneratorForClusterModeException extends RuntimeException {

  public UnsafeIdGeneratorForClusterModeException(IdGenerator resolvedIdGenerator) {
    super("cibseven.bpm.flow.job-executor.enabled=true requires a collision-resistant IdGenerator, "
        + "but the resolved IdGenerator is '" + describe(resolvedIdGenerator) + "'. CIB7's default "
        + "sequential DbIdGenerator reseeds on any freshly created schema (Postgres included, not "
        + "only H2/tests), which can collide with a stale Temporal workflow execution under "
        + "flow-job-executor's workflow-id scheme - see docs/cib7-fork-engine-plan.md §6 and §10 "
        + "risk 12. Set camunda.bpm.id-generator=strong (the engine's own default) or =prefixed, or "
        + "disable cluster mode (cibseven.bpm.flow.job-executor.enabled=false).");
  }

  private static String describe(IdGenerator resolvedIdGenerator) {
    return resolvedIdGenerator == null ? "null" : resolvedIdGenerator.getClass().getName();
  }

}
