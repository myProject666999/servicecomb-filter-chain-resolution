/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.servicecomb.core.filter;

import static org.apache.servicecomb.foundation.common.utils.StringBuilderUtils.appendLine;
import static org.apache.servicecomb.foundation.common.utils.StringBuilderUtils.deleteLast;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.servicecomb.config.ConfigurationChangedEvent;
import org.apache.servicecomb.foundation.common.event.EventManager;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.eventbus.Subscribe;

public class FilterChainsManager {
  /**
   * configuration items below this prefix affect filter chain resolution
   */
  public static final String FILTER_CONFIG_PREFIX = "servicecomb.filter.";

  private InvocationFilterChains consumerChains;

  private InvocationFilterChains providerChains;

  private InvocationFilterChains edgeChains;

  /**
   * invoked after the cached chains are invalidated by a configuration change,
   * listeners re-resolve the chains they own and push them to the already created
   * {@code MicroserviceMeta}s
   */
  private final List<Runnable> chainRefreshListeners = new CopyOnWriteArrayList<>();

  private final AtomicBoolean eventRegistered = new AtomicBoolean();

  @Autowired
  public FilterChainsManager setEdgeFilters(List<EdgeFilter> filters) {
    edgeChains = new InvocationFilterChains("edge filter chain", filters);
    return this;
  }

  @Autowired
  public FilterChainsManager setConsumerFilters(List<ConsumerFilter> filters) {
    consumerChains = new InvocationFilterChains("consumer filter chain", filters);
    return this;
  }

  @Autowired
  public FilterChainsManager setProviderFilters(List<ProviderFilter> filters) {
    providerChains = new InvocationFilterChains("provider filter chain", filters);
    return this;
  }

  public FilterChainsManager init() {
    if (eventRegistered.compareAndSet(false, true)) {
      EventManager.register(this);
    }
    return this;
  }

  public void destroy() {
    if (eventRegistered.compareAndSet(true, false)) {
      EventManager.unregister(this);
    }
  }

  public void addChainRefreshListener(Runnable listener) {
    chainRefreshListeners.add(listener);
  }

  /**
   * when the event is fired, the new values are already readable from Environment,
   * so simply invalidate and let the chains be re-resolved
   */
  @Subscribe
  public void onConfigurationChanged(ConfigurationChangedEvent event) {
    boolean filterRelated = event.getChanged().stream()
        .anyMatch(key -> key.startsWith(FILTER_CONFIG_PREFIX));
    if (!filterRelated) {
      return;
    }
    invalidateAndRefresh();
  }

  void invalidateAndRefresh() {
    if (consumerChains != null) {
      consumerChains.invalidate();
    }
    if (providerChains != null) {
      providerChains.invalidate();
    }
    if (edgeChains != null) {
      edgeChains.invalidate();
    }
    for (Runnable listener : chainRefreshListeners) {
      listener.run();
    }
  }

  public FilterNode findConsumerChain(String application, String serviceName) {
    return consumerChains.findChain(application, serviceName);
  }

  public FilterNode findProducerChain(String application, String serviceName) {
    return providerChains.findChain(application, serviceName);
  }

  public FilterNode findEdgeChain(String application, String serviceName) {
    return edgeChains.findChain(application, serviceName);
  }

  public String collectResolvedChains() {
    StringBuilder sb = new StringBuilder();

    collectChains(sb, "consumer", consumerChains);
    collectChains(sb, "producer", providerChains);
    collectChains(sb, "edge", edgeChains);

    return deleteLast(sb, 1).toString();
  }

  private void collectChains(StringBuilder sb, String title, InvocationFilterChains chains) {
    appendLine(sb, "%s: ", title);
    appendLine(sb, "  filters: %s", collectFilterNames(chains));
    if (hasOrderTie(chains)) {
      appendLine(sb, "  (*: same order, sequenced by filter name)");
    }
    Map<String, List<String>> resolved = chains.collectResolvedChains();
    if (!resolved.isEmpty()) {
      appendLine(sb, "  resolved chains: ");
      resolved.forEach((service, filters) -> appendLine(sb, "    %s: %s", service, String.join(" -> ", filters)));
    }
  }

  private boolean hasOrderTie(InvocationFilterChains chains) {
    return chains.getFilters().stream()
        .map(Filter::getOrder)
        .collect(Collectors.groupingBy(order -> order, Collectors.counting()))
        .values().stream().anyMatch(count -> count > 1);
  }

  // filters with the same global order are marked with "*", their final sequence
  // is decided by the filter name
  private List<String> collectFilterNames(InvocationFilterChains chains) {
    Map<Integer, Long> orderCounts = chains.getFilters().stream()
        .collect(Collectors.groupingBy(Filter::getOrder, Collectors.counting()));
    return chains.getFilters().stream()
        .map(filter -> filter.getName() + "(" + filter.getOrder() + ")"
            + (orderCounts.get(filter.getOrder()) > 1 ? "*" : ""))
        .collect(Collectors.toList());
  }
}
