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

import java.util.Date;

/**
 * One row of a process instance's merged audit timeline. {@code actorId} is {@code null}
 * whenever History itself can't attribute the entry to a user: a variable update only
 * resolves an actor when the command that produced it also logged a
 * {@code UserOperationLogEntry} sharing its operation id, which is not every command.
 */
public record AuditTimelineEntry(Date timestamp, String actorId, Type type, String description) {

  public enum Type {
    VARIABLE_UPDATE,
    TASK_CREATED,
    TASK_COMPLETED,
    COMMENT,
    CASE_EVENT
  }

}
