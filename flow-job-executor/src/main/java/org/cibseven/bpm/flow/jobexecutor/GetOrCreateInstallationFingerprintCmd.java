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

import java.util.UUID;

import org.cibseven.bpm.engine.impl.interceptor.Command;
import org.cibseven.bpm.engine.impl.interceptor.CommandContext;
import org.cibseven.bpm.engine.impl.persistence.entity.PropertyEntity;
import org.cibseven.bpm.engine.impl.persistence.entity.PropertyManager;

/**
 * Closes the workflow-id collision gap in docs/cib7-fork-engine-plan.md §6 / §10 risk 12: a
 * Temporal workflow id derived only from this engine's own job-row id can collide with a stale
 * execution from a <em>different incarnation</em> of this schema, because CIB7's default
 * {@code DbIdGenerator} reseeds its id watermark on any freshly created schema (confirmed true on
 * Postgres, not only H2/tests) - a dev/staging reset, a DR restore to an empty schema, or a
 * blue/green cutover to a new database all produce small, already-used-looking job ids again.
 *
 * <p>The fix: tie a random fingerprint to this schema's lifetime, not to the job-id sequence's.
 * It lives in the exact same {@code ACT_GE_PROPERTY} table the job-id watermark and
 * {@link RegisterOrVerifySharedTaskQueueCmd}'s queue-name record live in, so it is wiped and
 * regenerated in lockstep with every schema reset that would otherwise cause a collision, and
 * shared identically by every node of the same cluster (same schema) the way the queue name is -
 * two real nodes of one cluster must derive the <em>same</em> workflow id for the same job so
 * Temporal's dedupe still works across nodes, which is exactly what makes this schema-scoped
 * (not node-scoped) fingerprint correct.
 */
public class GetOrCreateInstallationFingerprintCmd implements Command<String> {

  static final String PROPERTY_NAME = "flow.jobExecutor.installationFingerprint";

  @Override
  public String execute(CommandContext commandContext) {
    PropertyManager propertyManager = commandContext.getPropertyManager();
    PropertyEntity existing = propertyManager.findPropertyById(PROPERTY_NAME);
    if (existing != null) {
      return existing.getValue();
    }

    String candidate = UUID.randomUUID().toString();
    try {
      propertyManager.insert(new PropertyEntity(PROPERTY_NAME, candidate));
      return candidate;
    } catch (RuntimeException raceLoser) {
      // Another node genuinely raced this same first-ever insert on this schema and committed
      // first (the NAME_ column is this table's primary key). Re-read what won instead of
      // treating this as success - every node must end up agreeing on one fingerprint.
      PropertyEntity winner = propertyManager.findPropertyById(PROPERTY_NAME);
      if (winner == null) {
        throw raceLoser;
      }
      return winner.getValue();
    }
  }

}
