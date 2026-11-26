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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.model.bpmn.instance.BaseElement;
import org.cibseven.bpm.model.bpmn.instance.ExtensionElements;
import org.cibseven.bpm.model.bpmn.instance.cibseven.CamundaProperties;
import org.cibseven.bpm.model.bpmn.instance.cibseven.CamundaProperty;
import org.cibseven.bpm.model.bpmn.BpmnModelInstance;

/**
 * Reads an {@link AiTaskConfig} off the current activity's {@code camunda:properties}
 * extension element, the same BPMN extension-element mechanism the disposition allowlist
 * spike verified: one {@code camunda:properties} block per element, read back through
 * {@code ExtensionElements -> CamundaProperties -> CamundaProperty}.
 */
public final class AiTaskConfigReader {

  private AiTaskConfigReader() {
  }

  public static AiTaskConfig readFrom(DelegateExecution execution) {
    BpmnModelInstance modelInstance = execution.getBpmnModelInstance();
    BaseElement element = modelInstance.getModelElementById(execution.getCurrentActivityId());

    Map<String, String> properties = new HashMap<>();
    ExtensionElements extensionElements = element.getExtensionElements();
    if (extensionElements != null) {
      CamundaProperties camundaProperties = extensionElements.getElementsQuery()
          .filterByType(CamundaProperties.class)
          .singleResult();
      if (camundaProperties != null) {
        for (CamundaProperty property : camundaProperties.getCamundaProperties()) {
          properties.put(property.getCamundaName(), property.getCamundaValue());
        }
      }
    }

    String sopPrompt = properties.getOrDefault("sopPrompt", "");
    String toolPathsValue = properties.get("toolPaths");
    List<String> toolPaths = (toolPathsValue == null || toolPathsValue.isBlank())
        ? List.of()
        : Arrays.stream(toolPathsValue.split(",")).map(String::trim).toList();
    double confidenceThreshold = Double.parseDouble(properties.getOrDefault("confidenceThreshold", "1.0"));
    String handoverQueue = properties.getOrDefault("handoverQueue", "");

    return new AiTaskConfig(sopPrompt, toolPaths, confidenceThreshold, handoverQueue);
  }

}
