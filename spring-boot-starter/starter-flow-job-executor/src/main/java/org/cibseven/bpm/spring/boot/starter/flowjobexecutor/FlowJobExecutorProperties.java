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

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(FlowJobExecutorProperties.PREFIX)
public class FlowJobExecutorProperties {

  public static final String PREFIX = "cibseven.bpm.flow.job-executor";

  /** Opts into {@code ClusterSafeJobExecutor} as the application's {@code JobExecutor}. */
  private boolean enabled = false;

  /** Temporal frontend target, e.g. {@code 127.0.0.1:7233}. */
  private String temporalTarget = "127.0.0.1:7233";

  /** Temporal namespace to dispatch workflows into. */
  private String temporalNamespace = "default";

  /**
   * One task-queue name shared by every node of this cluster - see
   * {@code ClusterSafeJobExecutor}'s own javadoc on why this must be identical across nodes.
   */
  private String taskQueue = "flow-job-executor";

  private long sweepIntervalMillis = 2000L;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getTemporalTarget() {
    return temporalTarget;
  }

  public void setTemporalTarget(String temporalTarget) {
    this.temporalTarget = temporalTarget;
  }

  public String getTemporalNamespace() {
    return temporalNamespace;
  }

  public void setTemporalNamespace(String temporalNamespace) {
    this.temporalNamespace = temporalNamespace;
  }

  public String getTaskQueue() {
    return taskQueue;
  }

  public void setTaskQueue(String taskQueue) {
    this.taskQueue = taskQueue;
  }

  public long getSweepIntervalMillis() {
    return sweepIntervalMillis;
  }

  public void setSweepIntervalMillis(long sweepIntervalMillis) {
    this.sweepIntervalMillis = sweepIntervalMillis;
  }

}
