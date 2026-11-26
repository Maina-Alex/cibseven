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

/**
 * What an {@link AiTaskExecutor} produced for one AI Task run: its output, a confidence
 * rating in {@code [0, 1]}, and the disposition it recommends (confirmed as-is on
 * auto-resolve, or offered for a human to confirm/override on handover).
 */
public record AiTaskResult(String output, double confidence, String recommendedDisposition) {
}
