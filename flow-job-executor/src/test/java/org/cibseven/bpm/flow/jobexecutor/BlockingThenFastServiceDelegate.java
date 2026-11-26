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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.JavaDelegate;

/**
 * First invocation blocks (simulating a worker that is genuinely mid-activity-execution, not a
 * node that crashed before starting one) until this test process interrupts it, to prove it is
 * really "in flight" before the test kills that worker; every later invocation - i.e. the retried
 * attempt actually executed by a surviving node - completes immediately, so the test observes a
 * real recovery rather than the same stuck attempt eventually finishing on its own.
 */
public class BlockingThenFastServiceDelegate implements JavaDelegate {

  private static final AtomicInteger invocationCount = new AtomicInteger(0);
  private static volatile CountDownLatch firstInvocationStarted = new CountDownLatch(1);

  @Override
  public void execute(DelegateExecution execution) throws Exception {
    int attempt = invocationCount.incrementAndGet();
    if (attempt == 1) {
      firstInvocationStarted.countDown();
      try {
        // Long enough that the test's forced worker kill (not this sleep expiring) is what ends
        // this attempt; a bare 120s wall-clock sleep rather than a latch so a genuine thread
        // interrupt (the worker shutdown) is what breaks it, same as a real crash would.
        Thread.sleep(TimeUnit.SECONDS.toMillis(120));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("simulated mid-activity worker crash", e);
      }
    }
    execution.setVariable("completedOnAttempt", attempt);
  }

  static boolean awaitFirstInvocationStarted(long timeoutSeconds) throws InterruptedException {
    return firstInvocationStarted.await(timeoutSeconds, TimeUnit.SECONDS);
  }

  static int invocationCount() {
    return invocationCount.get();
  }

  static void reset() {
    invocationCount.set(0);
    firstInvocationStarted = new CountDownLatch(1);
  }

}
