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

import org.cibseven.bpm.engine.ProcessEngine;
import org.cibseven.bpm.engine.impl.cfg.IdGenerator;
import org.cibseven.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.cibseven.bpm.engine.impl.db.DbIdGenerator;
import org.springframework.beans.factory.SmartInitializingSingleton;

/**
 * Fails application startup loudly, rather than letting an unsafe combination run silently, when
 * cluster mode (§10 risk 12) is enabled with an {@link IdGenerator} that is not safe for it.
 *
 * <p>Checks the real, fully-resolved {@link ProcessEngineConfigurationImpl#getIdGenerator()} on
 * the built {@link ProcessEngine} - not a heuristic over which Spring {@code IdGenerator} bean
 * happens to be present - because {@code camunda.bpm.id-generator=simple} registers no Spring bean
 * at all and falls through to the engine's own built-in {@link DbIdGenerator} default, which a
 * bean-presence check would miss entirely.
 *
 * <p>Implemented as a {@link SmartInitializingSingleton} rather than a plain
 * {@code @PostConstruct}/{@code InitializingBean} so this runs only after every singleton in the
 * context - including the {@code ProcessEngineFactoryBean} that actually builds the engine and
 * resolves its {@code IdGenerator} - has been instantiated, regardless of auto-configuration
 * ordering between this module and the base starter. A thrown exception here still propagates out
 * of {@code ApplicationContext} refresh, failing application startup.
 */
public class FlowJobExecutorClusterSafetyGuard implements SmartInitializingSingleton {

  private final ProcessEngine processEngine;

  public FlowJobExecutorClusterSafetyGuard(ProcessEngine processEngine) {
    this.processEngine = processEngine;
  }

  @Override
  public void afterSingletonsInstantiated() {
    IdGenerator idGenerator =
        ((ProcessEngineConfigurationImpl) processEngine.getProcessEngineConfiguration()).getIdGenerator();
    if (idGenerator instanceof DbIdGenerator) {
      throw new UnsafeIdGeneratorForClusterModeException(idGenerator);
    }
  }

}
