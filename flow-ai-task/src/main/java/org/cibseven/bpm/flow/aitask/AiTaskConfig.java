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
package org.cibseven.bpm.flow.aitask;

import java.util.List;

/**
 * Per-task configuration for an AI Task: the SOP prompt it operates against, which
 * {@code flow-tools} paths it may call, the confidence threshold that gates auto-resolve
 * vs. handover, and which Human Task queue to hand over to when confidence falls short.
 */
public record AiTaskConfig(String sopPrompt, List<String> toolPaths, double confidenceThreshold, String handoverQueue) {
}
