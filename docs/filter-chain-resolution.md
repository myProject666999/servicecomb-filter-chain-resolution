# 过滤器链的动态解析与失效

本文说明 `servicecomb.filter.<name>.<application>.<service>.order|enabled`
配置在运行期生效的实现、所做的取舍，以及不兼容的格式变化。

## 配置规则表

| 配置项 | 作用域 | 读取位置 | 非法值行为 |
| --- | --- | --- | --- |
| `servicecomb.filter.<name>.<app>.<service>.order` | 单个服务 | `AbstractFilter.getOrder(app, service)` | 重建失败：保留该服务上一条成功解析的链并告警（日志含配置键与服务名）；首次解析失败：抛 `FilterConfigException`，调用失败 |
| `servicecomb.filter.<name>.<app>.<service>.enabled` | 单个服务 | `AbstractFilter.enabledForMicroservice(app, service)` | 同上 |
| 过滤器的 `getOrder()` | 全局默认 | 无 per-service 配置时回退到此值 | — |

要点：

- 配置键按点号拼接，application 或 service 名中**带点会产生歧义**：
  `shop.cn`/`order` 与 `shop`/`cn.order` 对应同一个键
  `servicecomb.filter.retry.shop.cn.order.enabled`，该键对两条链同时生效
  （注册中心是否允许名称带点取决于注册中心实现，本模块不做假设）。
- 所有过滤器都对某服务禁用时，解析抛出 `IllegalStateException`
  （消息含链类型、application、service），不再出现下标越界。
- 链尾没有适用于当前 transport 的过滤器时（例如对 svc1 禁用了
  `rest-client-sender` 后发起 rest 调用），调用以失败的 `CompletableFuture`
  返回 `IllegalStateException`，消息含链身份（类型 + application/service）、
  过滤器名与 transport，不再是 `NullPointerException`。

## 顺序规则

1. 每个服务独立解析：`enabledForMicroservice` 过滤后，按
   `getOrder(application, serviceName)` 升序排列。
2. **同 order 按过滤器名称做二级排序**（新增，结论见下）。
3. `Filter.getOrder(String, String)` 的默认实现改为委托 `getOrder()`（见兼容性）。

### 二级排序的结论与影响

同 order 的先后此前取决于 Spring 注入 `List<ConsumerFilter>` 的顺序（即 bean
注册顺序），内置过滤器中存在两组并列：

- `retry` 与 `consumer-flow-control` 均为 `-1990`；
- `highway-client`、`rest-client-sender`、`websocket-client` 均为 `2000`
  （按 transport 互斥，相对顺序无实际影响）。

**结论：加名称二级排序**，使结果与 bean 注册顺序无关、可复现。影响：

- `consumer-flow-control` 固定在 `retry` **之前**：一次带重试的调用在流控中
  **计一次**（流控在重试之外）。若某部署当前的实际顺序相反（重试在外、每次
  重试都过流控），升级后计数口径会变化，需要知悉。
- 三个 transport sender 的相对顺序固定，但因互斥不影响行为。
- 并列会在启动摘要中以 `*` 标出（见下）。

## 失效策略的取舍

`ConfigurationChangedEvent` 发出时新值已写入 `Environment`。
`FilterChainsManager` 在 `init()` 时订阅该事件，变更键命中
`servicecomb.filter.` 前缀时触发失效与刷新。

**缓存失效：整体失效（代际标记），不按服务精确失效，也不物理清空。**

- 不按服务精确失效：配置键无法唯一反解出 (application, service)——名称可以
  带点（见规则表），一个键可能同时属于多条链。
- 不物理清空：清空会丢掉"上一条成功解析的链"，新配置非法时将没有可回退的
  链，且每次调用都会重新失败。
- 实现：`InvocationFilterChains` 持有单调递增的 `generation`，失效只做
  `generation++`；缓存条目携带解析时的代际，读到的条目代际落后于当前代际
  则重新解析。解析开始时记录代际，**在解析过程中发生失效**（如被栅栏挡住的
  线程读到旧值后继续）只会写入带旧代际的条目，下一次读取会再次解析——旧链
  不会被长期钉住；任何时刻调用方拿到的都是完整的旧链或新链。
- 重建失败（`FilterConfigException`）：保留旧链并把条目标记为当前代际，
  避免每次调用都重新执行失败的解析；下一次配置变更会再次触发重建。

**已创建的 `MicroserviceMeta`：回推新链（push），而不是每次调用向
`FilterChainsManager` 取链（pull）。**

- 事件处理时，`ReferenceConfigManager` 遍历全部已创建的
  `MicroserviceReferenceConfig`，重新解析并 `setConsumerFilterChain` /
  `setEdgeFilterChain`；`SCBEngine` 对生产端 `producerMicroserviceMeta`
  做同样的事。三个链字段改为 `volatile`，调用线程立即可见。
- 热点路径影响：调用路径（`InvokerUtils.invoke` /
  `ProducerInvocationFlow`）仍然是**一次 volatile 字段读**，与现状一致；
  pull 方案则要在每次调用时多做一次 volatile 读加两次
  `ConcurrentHashMap` 查询。代价转移到配置变更时：O(服务数 × 过滤器数)
  的重建，配置变更是低频事件，可接受。

## 可观测输出

### 启动摘要（`FilterChainsManager.collectResolvedChains`）

原来只列出 `getFilters()` 的原始列表与全局 `getOrder()`，看不到某个服务
实际解析出的链。现在：

```
consumer:
  filters: [consumer-flow-control(-1990)*, retry(-1990)*, load-balancer(0), ...]
  (*: same order, sequenced by filter name)
  resolved chains:
    shop/svc1: consumer-flow-control(-1990) -> retry(-1990) -> b(200) -> a(250) -> c(300)
```

- `*` 标出全局 order 并列的过滤器，其最终先后由名称决定；
- `resolved chains` 按服务列出已解析的链，order 为该服务的实际生效值
  （含 per-service 覆盖）。

### 阶段名（慢调用日志）

`AbstractFilter.getNameWithOrder` 原来把全局 order 格式化为
`F(%06d)-名称` 并缓存在实例上；一个过滤器实例被多个服务的链共用、且
per-service order 不同后，单个缓存字符串不再成立。现在阶段名由
`FilterNode` 在**建链时**按该服务的实际 order 计算并随节点保存
（`FilterNode.formatStageName(order, name)`）。

`SlowInvocationLogger` 按字典序输出阶段名，因此格式必须保证
**字典序 == 数值序**。旧格式对负数不成立（`F(-00500)` 排在 `F(-01990)`
前，与数值顺序相反）。新格式：

```
非负:  F(P<10位补零>)-<名称>      例: F(P0000000100)-myFilter
负数:  F(N<10位补零>)-<名称>      例: F(N2147481658)-retry
       数字为 order - Integer.MIN_VALUE，order 越大数字越大
```

`N` 字典序小于 `P`，全部负数排在非负数之前；组内定宽数字保持数值序。
整个 int 取值范围内字典序与数值序一致。

**格式变化（不兼容）**：慢调用日志中的阶段名从 `F(000100)-a` /
`F(-01990)-retry` 变为 `F(P0000000100)-a` / `F(N2147481658)-retry`。
依赖旧阶段名文本的日志解析规则需要同步调整。

## 兼容性说明

1. **`Filter.getOrder(String, String)` 默认实现由返回 `0` 改为委托
   `getOrder()`。** 直接实现 `ConsumerFilter`/`ProviderFilter`/`EdgeFilter`
   （未继承 `AbstractFilter`）的自定义过滤器，此前在按服务解析时一律按 0
   处理（紧跟 load-balance 之后），现在按 `getOrder()` 的实际值定位。例如
   `getOrder()` 为 1500 的 `audit`，此前排在 `consumer-public-key`(1010)
   之前，现在位于 `instance-bulkhead`(1060) 之后、`rest-client-codec`(1990)
   之前。继承 `AbstractFilter` 的过滤器（含全部内置过滤器）行为不变。
   依赖旧位置的自定义过滤器需要显式覆写 `getOrder(app, service)` 恢复。
2. **同 order 过滤器按名称定序**（见上），可能改变 `retry` 与
   `consumer-flow-control` 的相对顺序，从而影响流控对重试调用的计数口径。
3. **阶段名格式变化**（见上）。
4. 首次解析即遇到非法配置值时**沿用失败行为**（异常传播、调用失败）：
   此时没有已知良好的链可回退，静默回退全局顺序会掩盖配置笔误并产生
   出乎意料的顺序；运行期重建遇到非法值则保留旧链并告警。
