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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves and caches the filter chain per target microservice.
 *
 * <pre>
 * invalidation:
 *   configuration changes only bump {@link #generation}, cached entries are NOT removed.
 *   an entry whose generation differs from the current one is re-resolved lazily on
 *   the next access. this is chosen over clearing the map because:
 *     1. a configuration key can not be mapped back to one (application, serviceName)
 *        pair: names may contain dots, e.g. application "shop.cn" + service "order"
 *        and application "shop" + service "cn.order" share the key
 *        "servicecomb.filter.retry.shop.cn.order.enabled", so per-service invalidation
 *        is unreliable;
 *     2. keeping the stale entry gives a last-known-good chain to fall back to when
 *        the new configuration is invalid (see {@link FilterConfigException}).
 *
 * concurrency:
 *   a resolution reads the generation at its start and tags the cache entry with it.
 *   an entry written by a resolution that started before an invalidation is treated
 *   as stale and re-resolved on the next read, so a slow resolver can never pin an
 *   outdated chain. callers always get a complete old or new chain, never a partial one.
 * </pre>
 */
public class InvocationFilterChains {
  private static final Logger LOGGER = LoggerFactory.getLogger(InvocationFilterChains.class);

  /**
   * one resolved filter of a chain, with the order effective for the target microservice
   */
  public record ResolvedFilter(Filter filter, int order) {
    String stageName() {
      return FilterNode.formatStageName(order, filter.getName());
    }

    String display() {
      return filter.getName() + "(" + order + ")";
    }
  }

  record ChainKey(String application, String serviceName) {
    String display() {
      return application + "/" + serviceName;
    }
  }

  private record ChainEntry(long generation, FilterNode chain, List<ResolvedFilter> filters) {
  }

  private final String chainType;

  private final List<? extends Filter> filters;

  private final AtomicLong generation = new AtomicLong();

  private final Map<ChainKey, ChainEntry> microserviceChains = new ConcurrentHashMap<>();

  public InvocationFilterChains(String chainType, List<? extends Filter> filters) {
    this.chainType = chainType;
    this.filters = filters;
  }

  public List<? extends Filter> getFilters() {
    return filters;
  }

  /**
   * mark all cached chains stale, they will be re-resolved lazily
   */
  public void invalidate() {
    generation.incrementAndGet();
  }

  public FilterNode findChain(String application, String serviceName) {
    long currentGeneration = generation.get();
    ChainKey key = new ChainKey(application, serviceName);
    ChainEntry entry = microserviceChains.get(key);
    if (entry != null && entry.generation() == currentGeneration) {
      return entry.chain();
    }
    return resolveAndCache(key, currentGeneration, entry);
  }

  private FilterNode resolveAndCache(ChainKey key, long currentGeneration, ChainEntry staleEntry) {
    List<ResolvedFilter> resolved;
    try {
      resolved = resolve(key);
    } catch (FilterConfigException e) {
      if (staleEntry == null) {
        // first resolution fails fast, there is no known-good chain to fall back to
        throw e;
      }
      LOGGER.warn("failed to rebuild {} for {}, keep using the last resolved chain. {}",
          chainType, key.display(), e.getMessage());
      // tag the stale entry with the current generation, so callers do not
      // re-run the failing resolution on every invocation; the next
      // configuration change bumps the generation again and triggers a retry
      cacheIfNotSuperseded(key, new ChainEntry(currentGeneration, staleEntry.chain(), staleEntry.filters()));
      return staleEntry.chain();
    }

    FilterNode chain = buildChain(key, resolved);
    ChainEntry entry = new ChainEntry(currentGeneration, chain, resolved);
    return cacheIfNotSuperseded(key, entry).chain();
  }

  // never overwrite an entry written by a newer resolution
  private ChainEntry cacheIfNotSuperseded(ChainKey key, ChainEntry entry) {
    return microserviceChains.merge(key, entry,
        (existing, newer) -> existing.generation() >= newer.generation() ? existing : newer);
  }

  private List<ResolvedFilter> resolve(ChainKey key) {
    List<ResolvedFilter> resolved = new ArrayList<>();
    for (Filter filter : filters) {
      if (filter.enabledForMicroservice(key.application(), key.serviceName())) {
        resolved.add(new ResolvedFilter(filter, filter.getOrder(key.application(), key.serviceName())));
      }
    }
    if (resolved.isEmpty()) {
      throw new IllegalStateException(String.format(
          "no filter is enabled for %s, application=%s, service=%s, can not build the filter chain.",
          chainType, key.application(), key.serviceName()));
    }
    // sort by order, then by filter name so that equal orders get a deterministic
    // sequence independent of bean registration order
    resolved.sort(Comparator.comparingInt(ResolvedFilter::order)
        .thenComparing(resolvedFilter -> resolvedFilter.filter().getName()));
    return resolved;
  }

  private FilterNode buildChain(ChainKey key, List<ResolvedFilter> resolved) {
    String chainName = chainType + " for " + key.display();
    FilterNode head = null;
    FilterNode previous = null;
    for (ResolvedFilter resolvedFilter : resolved) {
      FilterNode node = new FilterNode(resolvedFilter.filter(), resolvedFilter.stageName(), chainName);
      if (previous == null) {
        head = node;
      } else {
        previous.setNextNode(node);
      }
      previous = node;
    }
    return head;
  }

  /**
   * snapshot of the currently resolved chains, for the boot up summary:
   * "application/service" -> ordered "name(order)" list
   */
  public Map<String, List<String>> collectResolvedChains() {
    Map<String, List<String>> result = new TreeMap<>();
    for (Map.Entry<ChainKey, ChainEntry> entry : microserviceChains.entrySet()) {
      result.put(entry.getKey().display(),
          entry.getValue().filters().stream().map(ResolvedFilter::display).toList());
    }
    return result;
  }
}
