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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.servicecomb.config.ConfigurationChangedEvent;
import org.apache.servicecomb.core.Invocation;
import org.apache.servicecomb.core.definition.MicroserviceMeta;
import org.apache.servicecomb.foundation.common.event.EventManager;
import org.apache.servicecomb.swagger.invocation.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

/**
 * Dynamic filter chain resolution tests. Only in-memory filters, a
 * {@link StandardEnvironment} backed by a map and hand made
 * {@link ConfigurationChangedEvent}s are used, no spring context is started.
 */
public class InvocationFilterChainsResolutionTest {
  static class TestFilter extends AbstractFilter implements ConsumerFilter, ProviderFilter, EdgeFilter {
    private final String name;

    private final int order;

    TestFilter(StandardEnvironment environment, String name, int order) {
      this.name = name;
      this.order = order;
      setEnvironment(environment);
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public int getOrder() {
      return order;
    }

    @Override
    public CompletableFuture<Response> onFilter(Invocation invocation, FilterNode nextNode) {
      if (nextNode == null) {
        return CompletableFuture.completedFuture(Response.ok(null));
      }
      return nextNode.onFilter(invocation);
    }
  }

  StandardEnvironment environment;

  Map<String, Object> source;

  TestFilter a;

  TestFilter b;

  TestFilter c;

  FilterChainsManager manager;

  List<FilterChainsManager> managersToDestroy;

  @Before
  public void setUp() {
    environment = new StandardEnvironment();
    source = new HashMap<>();
    // MapPropertySource wraps the map by reference, later mutations are visible
    environment.getPropertySources().addFirst(new MapPropertySource("test", source));

    // global order: a(100), b(200), c(300)
    a = new TestFilter(environment, "a", 100);
    b = new TestFilter(environment, "b", 200);
    c = new TestFilter(environment, "c", 300);

    managersToDestroy = new ArrayList<>();
    manager = newManager(List.of(a, b, c));
  }

  @After
  public void tearDown() {
    managersToDestroy.forEach(FilterChainsManager::destroy);
  }

  private FilterChainsManager newManager(List<? extends Filter> filters) {
    FilterChainsManager newManager = new FilterChainsManager()
        .setConsumerFilters(filters.stream().filter(ConsumerFilter.class::isInstance)
            .map(ConsumerFilter.class::cast).toList())
        .setProviderFilters(filters.stream().filter(ProviderFilter.class::isInstance)
            .map(ProviderFilter.class::cast).toList())
        .setEdgeFilters(filters.stream().filter(EdgeFilter.class::isInstance)
            .map(EdgeFilter.class::cast).toList())
        .init();
    managersToDestroy.add(newManager);
    return newManager;
  }

  private void changeConfig(Map<String, Object> updated) {
    source.putAll(updated);
    EventManager.post(ConfigurationChangedEvent.createIncremental(new HashMap<>(updated)));
  }

  private static List<String> chainNames(FilterNode chain) {
    List<String> names = new ArrayList<>();
    for (FilterNode node = chain; node != null; node = node.getNextNode()) {
      names.add(node.getFilter().getName());
    }
    return names;
  }

  @Test
  public void should_resolve_per_service_order_override() {
    source.put("servicecomb.filter.a.app.svc1.order", "250");

    assertThat(chainNames(manager.findConsumerChain("app", "svc1"))).containsExactly("b", "a", "c");
    // other services are not affected
    assertThat(chainNames(manager.findConsumerChain("app", "svc2"))).containsExactly("a", "b", "c");
  }

  @Test
  public void should_apply_runtime_disable_in_process() {
    source.put("servicecomb.filter.a.app.svc1.order", "250");
    assertThat(chainNames(manager.findConsumerChain("app", "svc1"))).containsExactly("b", "a", "c");

    changeConfig(Map.of("servicecomb.filter.b.app.svc1.enabled", "false"));

    assertThat(chainNames(manager.findConsumerChain("app", "svc1"))).containsExactly("a", "c");
    assertThat(chainNames(manager.findConsumerChain("app", "svc2"))).containsExactly("a", "b", "c");
  }

  @Test
  public void should_push_new_chain_to_existing_microservice_meta() {
    MicroserviceMeta microserviceMeta = new MicroserviceMeta(null, "app", "svc1", true);
    microserviceMeta.setConsumerFilterChain(manager.findConsumerChain("app", "svc1"));
    manager.addChainRefreshListener(
        () -> microserviceMeta.setConsumerFilterChain(manager.findConsumerChain("app", "svc1")));

    changeConfig(Map.of("servicecomb.filter.b.app.svc1.enabled", "false"));

    assertThat(chainNames(microserviceMeta.getConsumerFilterChain())).containsExactly("a", "c");
  }

  @Test
  public void should_ignore_unrelated_configuration_change() {
    assertThat(chainNames(manager.findConsumerChain("app", "svc1"))).containsExactly("a", "b", "c");

    changeConfig(Map.of("servicecomb.rest.address", "0.0.0.0:8080"));

    assertThat(chainNames(manager.findConsumerChain("app", "svc1"))).containsExactly("a", "b", "c");
  }

  @Test
  public void should_sort_direct_filter_by_its_getOrder_not_zero() {
    // a custom filter implementing ConsumerFilter directly, without AbstractFilter
    ConsumerFilter audit = new ConsumerFilter() {
      @Override
      public String getName() {
        return "audit";
      }

      @Override
      public int getOrder() {
        return 1500;
      }

      @Override
      public String getNameWithOrder() {
        return "audit";
      }

      @Override
      public CompletableFuture<Response> onFilter(Invocation invocation, FilterNode nextNode) {
        return nextNode.onFilter(invocation);
      }
    };
    TestFilter loadBalance = new TestFilter(environment, "load-balancer", 0);
    TestFilter publicKey = new TestFilter(environment, "consumer-public-key", 1010);
    TestFilter bulkhead = new TestFilter(environment, "instance-bulkhead", 1060);
    TestFilter codec = new TestFilter(environment, "rest-client-codec", 1990);

    FilterChainsManager customManager = newManager(List.of(loadBalance, publicKey, bulkhead, codec, audit));

    // audit(1500) sits between instance-bulkhead(1060) and rest-client-codec(1990),
    // not right after load-balancer(0)
    assertThat(chainNames(customManager.findConsumerChain("app", "svc1")))
        .containsExactly("load-balancer", "consumer-public-key", "instance-bulkhead", "audit", "rest-client-codec");
  }

  @Test
  public void should_break_order_ties_by_filter_name() {
    TestFilter retry = new TestFilter(environment, "retry", -1990);
    TestFilter flowControl = new TestFilter(environment, "consumer-flow-control", -1990);

    // even when retry is registered first, consumer-flow-control comes first by name
    FilterChainsManager customManager = newManager(List.of(retry, flowControl));

    assertThat(chainNames(customManager.findConsumerChain("app", "svc1")))
        .containsExactly("consumer-flow-control", "retry");
  }

  @Test
  public void should_keep_last_chain_when_new_config_value_is_invalid() {
    source.put("servicecomb.filter.a.app.svc1.order", "250");
    assertThat(chainNames(manager.findConsumerChain("app", "svc1"))).containsExactly("b", "a", "c");

    // "abc" can not be parsed as an integer: svc1 keeps the last resolved chain
    changeConfig(Map.of("servicecomb.filter.a.app.svc1.order", "abc"));
    assertThat(chainNames(manager.findConsumerChain("app", "svc1"))).containsExactly("b", "a", "c");

    // fix the value: the new chain is applied
    changeConfig(Map.of("servicecomb.filter.a.app.svc1.order", "50"));
    assertThat(chainNames(manager.findConsumerChain("app", "svc1"))).containsExactly("a", "b", "c");
  }

  @Test
  public void should_fail_fast_when_first_resolution_meets_invalid_config() {
    source.put("servicecomb.filter.a.app.svc1.order", "abc");

    assertThatThrownBy(() -> manager.findConsumerChain("app", "svc1"))
        .isInstanceOf(FilterConfigException.class)
        .hasMessageContaining("servicecomb.filter.a.app.svc1.order")
        .hasMessageContaining("svc1");
  }

  @Test
  public void should_fail_readably_when_all_filters_disabled_for_a_service() {
    changeConfig(Map.of(
        "servicecomb.filter.a.app.svc1.enabled", "false",
        "servicecomb.filter.b.app.svc1.enabled", "false",
        "servicecomb.filter.c.app.svc1.enabled", "false"));

    assertThatThrownBy(() -> manager.findConsumerChain("app", "svc1"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("app")
        .hasMessageContaining("svc1");
  }

  @Test
  public void should_fail_readably_when_all_filters_disabled_at_runtime() {
    assertThat(chainNames(manager.findConsumerChain("app", "svc1"))).containsExactly("a", "b", "c");

    changeConfig(Map.of(
        "servicecomb.filter.a.app.svc1.enabled", "false",
        "servicecomb.filter.b.app.svc1.enabled", "false",
        "servicecomb.filter.c.app.svc1.enabled", "false"));

    // no half built chain is cached, every call fails with the same readable error
    for (int idx = 0; idx < 2; idx++) {
      assertThatThrownBy(() -> manager.findConsumerChain("app", "svc1"))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("svc1");
    }
  }

  @Test
  public void should_rebuild_chains_of_dotted_names_sharing_one_config_key() {
    // application "shop.cn" + service "order" and application "shop" + service "cn.order"
    // map to the same configuration key
    assertThat(chainNames(manager.findConsumerChain("shop.cn", "order"))).containsExactly("a", "b", "c");
    assertThat(chainNames(manager.findConsumerChain("shop", "cn.order"))).containsExactly("a", "b", "c");

    changeConfig(Map.of("servicecomb.filter.a.shop.cn.order.enabled", "false"));

    assertThat(chainNames(manager.findConsumerChain("shop.cn", "order"))).containsExactly("b", "c");
    assertThat(chainNames(manager.findConsumerChain("shop", "cn.order"))).containsExactly("b", "c");
  }

  @Test
  public void should_list_resolved_chains_and_mark_order_ties_in_summary() {
    TestFilter retry = new TestFilter(environment, "retry", -1990);
    TestFilter flowControl = new TestFilter(environment, "consumer-flow-control", -1990);
    FilterChainsManager customManager = newManager(List.of(retry, flowControl, a, b, c));

    source.put("servicecomb.filter.a.app.svc1.order", "250");
    customManager.findConsumerChain("app", "svc1");

    String summary = customManager.collectResolvedChains();
    assertThat(summary)
        .contains("retry(-1990)*")
        .contains("consumer-flow-control(-1990)*")
        .contains("app/svc1: consumer-flow-control(-1990) -> retry(-1990) -> b(200) -> a(250) -> c(300)");
  }

  /**
   * thread R resolves the chain of svc1 for the first time and is blocked inside
   * filter a's enabledForMicroservice after reading the old value true; the
   * configuration then disables a for svc1 and the change event is fully handled
   * before R continues. R must not pin the old chain.
   */
  @Test
  public void should_eventually_apply_new_chain_when_resolution_races_with_config_change() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicBoolean blockOnce = new AtomicBoolean(true);

    TestFilter blockingA = new TestFilter(environment, "a", 100) {
      @Override
      public boolean enabledForMicroservice(String application, String serviceName) {
        boolean enabled = super.enabledForMicroservice(application, serviceName);
        if (blockOnce.compareAndSet(true, false)) {
          entered.countDown();
          try {
            release.await(10, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
        }
        return enabled;
      }
    };
    FilterChainsManager customManager = newManager(List.of(blockingA, b, c));

    MicroserviceMeta microserviceMeta = new MicroserviceMeta(null, "app", "svc1", true);
    customManager.addChainRefreshListener(
        () -> microserviceMeta.setConsumerFilterChain(customManager.findConsumerChain("app", "svc1")));

    AtomicReference<FilterNode> resolvedByR = new AtomicReference<>();
    AtomicReference<Throwable> errorOfR = new AtomicReference<>();
    Thread threadR = new Thread(() -> {
      try {
        resolvedByR.set(customManager.findConsumerChain("app", "svc1"));
      } catch (Throwable e) {
        errorOfR.set(e);
      }
    }, "resolver-R");
    threadR.start();

    // R has read enabled=true for a and is blocked now
    assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();

    // disable a for svc1; the event is handled synchronously: caches are
    // invalidated and the new chain is pushed to the microservice meta
    changeConfig(Map.of("servicecomb.filter.a.app.svc1.enabled", "false"));
    assertThat(chainNames(microserviceMeta.getConsumerFilterChain())).containsExactly("b", "c");

    // let R finish its stale resolution
    release.countDown();
    threadR.join(10_000);
    assertThat(errorOfR.get()).isNull();

    // R's stale resolution can not pin the old chain: the cache keeps the chain
    // installed by the event handler, and R also observes that newer complete chain
    assertThat(chainNames(resolvedByR.get())).containsExactly("b", "c");
    assertThat(chainNames(microserviceMeta.getConsumerFilterChain())).containsExactly("b", "c");
    assertThat(chainNames(customManager.findConsumerChain("app", "svc1"))).containsExactly("b", "c");
  }
}
