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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.servicecomb.core.Invocation;
import org.apache.servicecomb.foundation.common.utils.AsyncUtils;
import org.apache.servicecomb.swagger.invocation.Response;

public class FilterNode {
  public static final FilterNode EMPTY = new FilterNode(null) {
    @Override
    public CompletableFuture<Response> onFilter(Invocation invocation) {
      return CompletableFuture.completedFuture(Response.ok(null));
    }
  };

  /**
   * Build the stage name for a filter with the given order.
   *
   * <p>Stage names are collected into a map keyed by the name and are printed
   * sorted by name (e.g. by the slow invocation logger), so the lexicographic
   * order of the formatted string MUST match the numeric order of {@code order},
   * for the whole int range including negatives:</p>
   * <pre>
   *   non-negative: F(P&lt;zero padded 10 digits&gt;)-&lt;name&gt;,   e.g. F(P0000000100)-myFilter
   *   negative:     F(N&lt;zero padded 10 digits&gt;)-&lt;name&gt;,   e.g. F(N2147481658)-retry
   *                 the digits are {@code order - Integer.MIN_VALUE}, i.e. bigger order, bigger digits
   * </pre>
   * <p>{@code N} sorts before {@code P}, so all negative orders sort before all
   * non-negative ones, and inside each group the fixed width digits keep the
   * numeric order.</p>
   */
  public static String formatStageName(int order, String name) {
    if (order < 0) {
      return String.format("F(N%010d)-%s", (long) order - Integer.MIN_VALUE, name);
    }
    return String.format("F(P%010d)-%s", order, name);
  }

  public static FilterNode buildChain(Filter... filters) {
    return buildChain(Arrays.asList(filters));
  }

  public static FilterNode buildChain(List<Filter> filters) {
    if (filters.isEmpty()) {
      throw new IllegalStateException("can not build filter chain from an empty filter list.");
    }

    List<FilterNode> filterNodes = filters.stream()
        .map(FilterNode::new).toList();

    for (int idx = 0; idx < filterNodes.size() - 1; idx++) {
      FilterNode currentNode = filterNodes.get(idx);
      FilterNode nextNode = filterNodes.get(idx + 1);
      currentNode.setNextNode(nextNode);
    }

    return filterNodes.get(0);
  }

  private final Filter filter;

  // stage name resolved for the chain this node belongs to, may carry a
  // per-microservice customized order; null means "use filter.getNameWithOrder()"
  private final String stageName;

  // human readable chain identity, e.g. "consumer filter chain for shop/svc1",
  // used in error messages
  private String chainName = "unknown filter chain";

  private FilterNode nextNode;

  public FilterNode(Filter filter) {
    this.filter = filter;
    this.stageName = null;
  }

  public FilterNode(Filter filter, String stageName, String chainName) {
    this.filter = filter;
    this.stageName = stageName;
    this.chainName = chainName;
  }

  public Filter getFilter() {
    return filter;
  }

  public FilterNode getNextNode() {
    return nextNode;
  }

  void setNextNode(FilterNode nextNode) {
    this.nextNode = nextNode;
  }

  public String getStageName() {
    return stageName != null ? stageName : filter.getNameWithOrder();
  }

  public CompletableFuture<Response> onFilter(Invocation invocation) {
    // When transport name is empty, maybe edge transport filters need to be executed.
    // Can't set Endpoint before load balance in edge.
    if (invocation.getTransportName() != null && !filter.enabledForTransport(invocation.getTransportName())) {
      if (nextNode == null) {
        return noApplicableNodeFuture(invocation);
      }
      return nextNode.onFilter(invocation);
    }

    String stage = invocation.getInvocationStageTrace().recordStageBegin(getStageName());
    return AsyncUtils.tryCatchSupplierFuture(() -> filter.onFilter(invocation, nextNode)
        .whenComplete((r, e) -> invocation.getInvocationStageTrace().recordStageEnd(stage)));
  }

  // the tail filters of a chain are transport specific (e.g. rest-client-sender),
  // when none of them applies to the current transport, fail with a readable
  // message instead of a NullPointerException
  private CompletableFuture<Response> noApplicableNodeFuture(Invocation invocation) {
    CompletableFuture<Response> future = new CompletableFuture<>();
    future.completeExceptionally(new IllegalStateException(String.format(
        "no filter node applicable to transport [%s] after filter [%s] in %s, "
            + "maybe all transport sender filters are disabled for the target microservice.",
        invocation.getTransportName(), filter.getName(), chainName)));
    return future;
  }
}
