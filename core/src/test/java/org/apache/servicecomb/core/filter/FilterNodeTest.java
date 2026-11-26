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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.servicecomb.core.Invocation;
import org.apache.servicecomb.core.invocation.InvocationStageTrace;
import org.apache.servicecomb.swagger.invocation.Response;
import org.junit.Test;
import org.springframework.core.env.StandardEnvironment;

public class FilterNodeTest {
  static class TransportFilter extends AbstractFilter implements ConsumerFilter {
    private final String name;

    private final int order;

    private final String transport;

    TransportFilter(StandardEnvironment environment, String name, int order, String transport) {
      this.name = name;
      this.order = order;
      this.transport = transport;
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
    public boolean enabledForTransport(String transport) {
      return this.transport == null || this.transport.equals(transport);
    }

    @Override
    public CompletableFuture<Response> onFilter(Invocation invocation, FilterNode nextNode) {
      return nextNode.onFilter(invocation);
    }
  }

  @Test
  public void stage_name_lexicographic_order_should_match_numeric_order() {
    List<Integer> orders = List.of(Integer.MIN_VALUE, -1990, -500, -1, 0, 100, 250, 1990, Integer.MAX_VALUE);

    List<String> stageNames = orders.stream()
        .map(order -> FilterNode.formatStageName(order, "filter"))
        .toList();
    List<String> sorted = new ArrayList<>(stageNames);
    Collections.sort(sorted);

    assertThat(sorted).isEqualTo(stageNames);
  }

  @Test
  public void stage_name_format_should_be_fixed_width_and_signed() {
    assertThat(FilterNode.formatStageName(100, "a")).isEqualTo("F(P0000000100)-a");
    assertThat(FilterNode.formatStageName(-1990, "retry")).isEqualTo("F(N2147481658)-retry");
  }

  @Test
  public void should_fail_readably_when_build_chain_from_empty_list() {
    assertThatThrownBy(() -> FilterNode.buildChain(List.of()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("empty");
  }

  @Test
  public void should_fail_readably_when_no_tail_filter_applicable_to_transport() {
    StandardEnvironment environment = new StandardEnvironment();
    // rest-client-codec applies to all transports, rest-client-sender only to rest;
    // with the sender disabled for svc1, a rest call would fall off the chain tail
    TransportFilter codec = new TransportFilter(environment, "rest-client-codec", 1990, null);
    TransportFilter sender = new TransportFilter(environment, "rest-client-sender", 2000, "rest");

    InvocationFilterChains chains = new InvocationFilterChains("consumer filter chain", List.of(codec, sender));
    FilterNode chain = chains.findChain("app", "svc1");

    Invocation invocation = mock(Invocation.class);
    when(invocation.getTransportName()).thenReturn("highway");
    when(invocation.getInvocationStageTrace()).thenReturn(new InvocationStageTrace(invocation));

    assertThatThrownBy(() -> chain.onFilter(invocation).get())
        .isInstanceOf(ExecutionException.class)
        .cause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("highway")
        .hasMessageContaining("rest-client-sender")
        .hasMessageContaining("app/svc1");
  }

  @Test
  public void resolved_chain_nodes_should_carry_per_service_stage_names() {
    StandardEnvironment environment = new StandardEnvironment();
    TransportFilter a = new TransportFilter(environment, "a", 100, null);
    TransportFilter b = new TransportFilter(environment, "b", 200, null);

    InvocationFilterChains chains = new InvocationFilterChains("consumer filter chain", List.of(a, b));
    FilterNode chain = chains.findChain("app", "svc1");

    assertThat(chain.getStageName()).isEqualTo("F(P0000000100)-a");
    assertThat(chain.getNextNode().getStageName()).isEqualTo("F(P0000000200)-b");
  }
}
