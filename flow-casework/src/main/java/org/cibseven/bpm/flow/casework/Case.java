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
package org.cibseven.bpm.flow.casework;

import java.util.Date;

/**
 * A durable unit of work that owns zero, one, or many BPMN process instances and their tasks
 * over its lifetime, never the reverse — a case is never 1:1 with a single execution.
 */
public class Case {

  public enum Status {
    OPEN,
    CLOSED,
    REOPENED
  }

  private final String id;
  private final String title;
  private final Date openedAt;
  private Status status;
  private Date closedAt;

  public Case(String id, String title, Date openedAt) {
    this.id = id;
    this.title = title;
    this.openedAt = openedAt;
    this.status = Status.OPEN;
  }

  public String id() {
    return id;
  }

  public String title() {
    return title;
  }

  public Date openedAt() {
    return openedAt;
  }

  public Status status() {
    return status;
  }

  public Date closedAt() {
    return closedAt;
  }

  void close(Date closedAt) {
    this.status = Status.CLOSED;
    this.closedAt = closedAt;
  }

  void reopen() {
    this.status = Status.REOPENED;
    this.closedAt = null;
  }

}
