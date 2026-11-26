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
package org.cibseven.bpm.flow.jobexecutor;

import org.cibseven.bpm.engine.impl.interceptor.Command;
import org.cibseven.bpm.engine.impl.interceptor.CommandContext;
import org.cibseven.bpm.engine.impl.persistence.entity.PropertyEntity;
import org.cibseven.bpm.engine.impl.persistence.entity.PropertyManager;

/**
 * Structural guard against the exact misconfiguration that would silently reintroduce the
 * multi-node job-loss gap this module closes (docs/cib7-fork-engine-plan.md §10 risk 12): a node
 * started with a different {@code sharedTaskQueueName} than its siblings would never share work
 * or orphan recovery with them, and nothing about a merely-different constructor argument would
 * surface that until a job was actually lost.
 *
 * <p>Uses the engine's own {@code ACT_GE_PROPERTY} table (the same shared database every node in
 * the cluster already talks to via {@link org.cibseven.bpm.engine.impl.cmd.AcquireJobsCmd}) as the
 * one canonical, discoverable record of which queue name this cluster has committed to. The first
 * node to start records it; every subsequent node - on this process or any other - must match it
 * exactly, or refuses to start with a loud, specific exception rather than silently running with a
 * divergent queue name.
 */
public class RegisterOrVerifySharedTaskQueueCmd implements Command<Void> {

  static final String PROPERTY_NAME = "flow.jobExecutor.sharedTaskQueueName";

  private final String sharedTaskQueueName;

  public RegisterOrVerifySharedTaskQueueCmd(String sharedTaskQueueName) {
    this.sharedTaskQueueName = sharedTaskQueueName;
  }

  @Override
  public Void execute(CommandContext commandContext) {
    PropertyManager propertyManager = commandContext.getPropertyManager();
    PropertyEntity existing = propertyManager.findPropertyById(PROPERTY_NAME);

    if (existing == null) {
      boolean inserted;
      try {
        propertyManager.insert(new PropertyEntity(PROPERTY_NAME, sharedTaskQueueName));
        inserted = true;
      } catch (RuntimeException raceLoser) {
        // Another node's start() genuinely raced this same insert and committed first (the
        // NAME_ column is this table's primary key). Re-read what won the race and fall through
        // to the ordinary mismatch check below instead of treating this as success.
        inserted = false;
        existing = propertyManager.findPropertyById(PROPERTY_NAME);
        if (existing == null) {
          // The row still isn't visible (e.g. the race loser's own transaction hasn't seen the
          // winner's commit yet under this isolation level) - surface the original failure
          // rather than silently proceeding with an unverified queue name.
          throw raceLoser;
        }
      }
      if (inserted) {
        return null;
      }
    }

    if (!existing.getValue().equals(sharedTaskQueueName)) {
      throw new IllegalStateException(
          "ClusterSafeJobExecutor refuses to start: this node is configured with shared task "
              + "queue name '" + sharedTaskQueueName + "', but this cluster's engine database "
              + "already has '" + existing.getValue() + "' registered under the '" + PROPERTY_NAME
              + "' property (set by whichever node started first). A per-node queue-name "
              + "divergence is exactly the misconfiguration that would silently reintroduce the "
              + "multi-node job-loss gap in docs/cib7-fork-engine-plan.md §10 risk 12 - fix the "
              + "configuration of one of the two nodes so they agree, rather than starting with "
              + "mismatched names.");
    }
    return null;
  }

}
