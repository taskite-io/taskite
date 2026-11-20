# Placement Driver 运维 API

本文档描述 `placement-driver` 模块当前提供的 HTTP 运维与观测接口。接口由
`PlacementDriverAdminController` 暴露，`PlacementDriverEndpointManager` 将请求路由到本地
command submitter 或 Leader gRPC，业务决策由对应 Processor 闭环完成。

以下示例使用：

```bash
BASE=http://127.0.0.1:19001
PD=$BASE/api/v1/pd
BROKER=$BASE/api/v1/broker
WORKERS=$BASE/api/v1/workers
CLUSTER=demo
```

## 配置

默认配置和发行模板分别位于：

- `dist/src/main/resources/application.yaml`：可直接运行的默认配置，会打入 dist JAR。
- `dist/src/main/config/placement-driver.yaml.template`：包含全部 PD 配置项、环境变量和格式说明，
  会复制到 `dist/target/config/placement-driver.yaml.template`。

Spring Boot 会按标准优先级合并 YAML、环境变量和命令行参数。常用环境变量如下：

|                    环境变量                    |                  YAML 配置                   |
|--------------------------------------------|--------------------------------------------|
| `SERVER_PORT`                              | `server.port`                              |
| `TASKITE_PD_NODE_ID`                       | `taskite.pd.node-id`                       |
| `TASKITE_PD_NETWORK_HOST`                  | `taskite.pd.network.host`                  |
| `TASKITE_PD_NETWORK_GRPC_PORT`             | `taskite.pd.network.grpc-port`             |
| `TASKITE_PD_NETWORK_RAFT_PORT`             | `taskite.pd.network.raft-port`             |
| `TASKITE_PD_CONSENSUS_ELECTION_TIMEOUT`    | `taskite.pd.consensus.election-timeout`    |
| `TASKITE_PD_CONSENSUS_HEARTBEAT_INTERVAL`  | `taskite.pd.consensus.heartbeat-interval`  |
| `TASKITE_PD_BROKER_LEASE_TIMEOUT`          | `taskite.pd.broker.lease-timeout`          |
| `TASKITE_PD_BROKER_CLIENT_REQUEST_TIMEOUT` | `taskite.pd.broker.client.request-timeout` |
| `TASKITE_PD_BROKER_CLIENT_MAX_RETRIES`     | `taskite.pd.broker.client.max-retries`     |
| `TASKITE_PD_WORKER_LEASE_TIMEOUT`          | `taskite.pd.worker.lease-timeout`          |
| `TASKITE_PD_WORKER_OFFLINE_RETENTION`      | `taskite.pd.worker.offline-retention`      |
| `TASKITE_PD_STORAGE_DATA_DIR`              | `taskite.pd.storage.data-dir`              |
| `TASKITE_PD_CONSENSUS_BOOTSTRAP_PEERS`     | `taskite.pd.consensus.bootstrap-peers`     |

所有时间值使用带单位的 Duration，例如 `300ms`、`5s`、`10m`。

`TASKITE_PD_CONSENSUS_BOOTSTRAP_PEERS` 使用逗号分隔的 `nodeId@host:grpcPort:raftPort`，且只列出其他 PD 节点。它只负责
首次 genesis 和无本地 manifest 时的 seed discovery；正常重启优先读取
`${taskite.pd.storage.data-dir}/.topology`，再由 JRaft snapshot/replay 恢复权威状态。外部配置可通过
`--spring.config.location` 替换默认位置，或通过 `--spring.config.additional-location`
追加。使用模板时，先将其重命名为 `application.yaml`：

多机部署时，`TASKITE_PD_NETWORK_HOST` 必须是其他 PD 节点可达的本机地址，不能使用 `127.0.0.1` 或
`0.0.0.0`；仅同机多进程集群可以统一使用 loopback。`TASKITE_PD_NETWORK_GRPC_PORT` 面向控制面客户端，
`TASKITE_PD_NETWORK_RAFT_PORT` 仅承载 PD 节点间复制与选举。两者必须显式配置且不能相同，因为它们的连通性、
负载、超时和访问控制要求不同，也不会在同一个 gRPC Server 上复用。所有节点的 `TASKITE_PD_NETWORK_HOST`、
`TASKITE_PD_NETWORK_GRPC_PORT`、`TASKITE_PD_NETWORK_RAFT_PORT` 和 bootstrap peers 必须描述同一组成员端点，否则各节点会持续
pre-vote 而无法形成 Leader。

```bash
java -jar dist/target/taskite-distribution-0.1.0-SNAPSHOT.jar placement-driver \
  --spring.config.additional-location=optional:file:dist/target/config/

java -jar dist/target/taskite-distribution-0.1.0-SNAPSHOT.jar placement-driver \
  --server.port=19004 \
  --taskite.pd.node-id=meta-3 \
  --taskite.pd.network.host=127.0.0.1 \
  --taskite.pd.network.grpc-port=26507 \
  --taskite.pd.network.raft-port=26508 \
  --taskite.pd.storage.data-dir=/tmp/placement-driver-fixed/meta-3-rejoin \
  --taskite.pd.consensus.bootstrap-peers=meta-0@127.0.0.1:26501:26502,meta-1@127.0.0.1:26503:26504,meta-2@127.0.0.1:26505:26506
```

## 接口总览

|    方法    |                                       路径                                       |           用途            |
|----------|--------------------------------------------------------------------------------|-------------------------|
| `GET`    | `/api/v1/pd/health`                                                            | 查询当前 PD 节点健康和 Raft 状态   |
| `GET`    | `/api/v1/pd/topology`                                                          | 查询 PD 成员、角色与 Leader 拓扑  |
| `GET`    | `/api/v1/pd/configuration`                                                     | 查询 PD 目标与实际成员配置         |
| `PATCH`  | `/api/v1/pd/configuration`                                                     | 按 add/remove/count 变更成员 |
| `GET`    | `/api/v1/pd/changes/active`                                                    | 查询当前 PD 成员变更            |
| `GET`    | `/api/v1/pd/changes/{changeId}`                                                | 查询指定 PD 成员变更            |
| `GET`    | `/api/v1/workers`                                                              | 查询 Worker 注册与存活状态       |
| `POST`   | `/api/v1/broker/clusters`                                                      | 创建 Broker 集群配置          |
| `GET`    | `/api/v1/broker/clusters`                                                      | 查询当前已有 Broker 集群        |
| `DELETE` | `/api/v1/broker/clusters/{clusterId}`                                          | 删除空集群或全 OFFLINE 集群      |
| `GET`    | `/api/v1/broker/clusters/{clusterId}/configuration`                            | 查询集群配置                  |
| `PATCH`  | `/api/v1/broker/clusters/{clusterId}/configuration`                            | 统一调整 Broker、分区和副本因子     |
| `GET`    | `/api/v1/broker/clusters/{clusterId}/topology`                                 | 查询路由拓扑与 Broker 分区观测     |
| `POST`   | `/api/v1/broker/clusters/{clusterId}/rebalance`                                | 显式触发 placement 重算       |
| `POST`   | `/api/v1/broker/clusters/{clusterId}/partitions/{partitionId}/quorum-recovery` | 灾难恢复                    |
| `GET`    | `/api/v1/broker/clusters/{clusterId}/changes/active`                           | 查询当前在途聚合变更              |
| `GET`    | `/api/v1/broker/clusters/{clusterId}/changes/{changeId}`                       | 查询指定聚合变更                |
| `DELETE` | `/api/v1/broker/clusters/{clusterId}/changes/{changeId}`                       | 取消指定聚合变更                |
| `POST`   | `/api/v1/broker/job-subscriptions`                                             | 创建普通任务订阅并受理分发           |
| `GET`    | `/api/v1/broker/job-subscriptions`                                             | 列出原生及 Mirror 订阅分发       |
| `GET`    | `/api/v1/broker/job-subscriptions/{distributionKey}`                           | 查询创建分发的模板、轮次和 ACK       |
| `POST`   | `/api/v1/broker/job-subscriptions/{distributionKey}/redistribute`              | 按预期轮次重新下发               |
| `DELETE` | `/api/v1/broker/job-subscriptions/{subscriptionKey}`                           | 按作用域受理普通订阅删除            |
| `POST`   | `/api/v1/broker/timer-subscriptions`                                           | 创建独立定时订阅                |
| `GET`    | `/api/v1/broker/timer-subscriptions`                                           | 查询当前仍在调度的定时订阅           |
| `DELETE` | `/api/v1/broker/timer-subscriptions/{timerSubscriptionKey}`                    | 按作用域取消定时订阅              |
| `POST`   | `/api/v1/broker/job-subscription-mirrors`                                      | 创建 Mirror 关系            |
| `GET`    | `/api/v1/broker/job-subscription-mirrors`                                      | 查询 Mirror 关系列表          |
| `GET`    | `/api/v1/broker/job-subscription-mirrors/{mirrorKey}`                          | 查询指定 Mirror 关系          |
| `DELETE` | `/api/v1/broker/job-subscription-mirrors/{mirrorKey}`                          | 删除指定 Mirror 关系          |

写请求应发送到 PD Leader。无法转发到 Leader 时返回 `503 NOT_LEADER`，响应中的
`leaderEndpoint` 给出已知 Leader 地址。错误响应通常包含：

```json
{
  "error": "ERROR_CODE",
  "message": "diagnostic message",
  "leaderEndpoint": "127.0.0.1:29001",
  "pendingChangeIds": []
}
```

## 3.1 Worker 注册与存活看板

Worker 与 Broker 注册使用
`placementDriverProtocol.PlacementDriver` 的 generated gRPC stub。可读协议定义位于
`placement-driver-protocol/src/main/resources/placement_driver.proto`，PD gRPC Mapper 将
请求转换为内部 SBE Record，再通过原有 Processor 和 Raft 状态机提交。该服务复用
`TASKITE_PD_NETWORK_GRPC_PORT`（默认 `26501`），不增加独立监听端口。

Go SDK 在 Worker `Open` 后调用 `RegisterWorker`，之后周期调用 `RenewWorker`，并在
`Close` 时调用 `DeregisterWorker`。Broker 的注册、续约和注销同样使用该 generated stub。

心跳命令经 PD Raft 复制。Leader 根据最后续约时间和
`taskite.pd.broker.lease-timeout`、`taskite.pd.worker.lease-timeout` 生成统一 Deadline
索引；切主或重启后从 Registration 重建索引。客户端上报并持久化的
`renewalTimeoutMillis` 仅用于诊断，不参与服务端 Deadline 计算。过期操作带预期心跳时间
fence，不会覆盖已提交的新心跳。

Worker 转为 OFFLINE 时会写入
`statusChangedAt + pd.worker.offline-retention` 的清理 Deadline，不再周期扫描全部
Worker。Deadline 到期后通过 Raft `DELETE` 命令删除，并以 `statusChangedAt` fencing，避免
删除重新上线的 Worker。所有 PD 节点必须使用相同的 Broker lease、Worker lease 和
offline retention 配置。

Worker 看板由 Prometheus 指标和 Grafana 提供，dashboard JSON 位于：

```text
monitor/grafana/workers.json
```

查询 API 支持 `cluster`、`tenant`、`name` 三个可选精确筛选维度：

```bash
curl "$WORKERS?cluster=demo&tenant=%3Cdefault%3E&name=payment-worker"
```

响应包含在线/离线汇总及每个物理 Worker 实例的 `workerId`、Job type、实时
active/max job 容量、并发数、拉取超时、变量筛选、SDK 版本、注册时间和最近续约。多个
tenant 会展开为多条注册记录；`workerId` 用于区分同名横向实例。

Prometheus 暴露 `taskite_pd_worker_status`、`taskite_pd_worker_active_jobs`、
`taskite_pd_worker_max_jobs_active` 和 `taskite_pd_worker_last_renewal_at_seconds`，
通用标签包含 `cluster`、`tenant`、`name`、`worker_id`、`job_type` 和
`client_version`，最近续约指标额外包含 `status`。Worker 指标只由当前 PD Leader 发布；
Grafana 查询同时按 `taskite_pd_raft_role == 3` 过滤，避免 Leader 切换期间混入 follower
的重复序列。

Broker 最近续约详情由 `taskite_pd_broker_last_renewal_at_seconds` 暴露，包含状态、主机、
Command/Raft 端口和版本标签。主 Dashboard 的 `Registered Brokers` 表格只读取当前 PD
Leader，并支持按 `cluster` 和 `broker` 变量筛选。

## 3.2 健康状态

```bash
curl "$PD/health"
```

返回当前节点角色、Leader、term、quorum、commit index、last applied、HTTP/gRPC 端口等。

## 3.3 PD 拓扑

```bash
curl "$PD/topology"
```

返回 `localMemberId`、Leader、term、quorum 和 PD 成员列表。成员项包含 HTTP/gRPC/Raft
endpoint、role、status、incarnation/endpoint epoch、复制进度、晋升就绪状态和 `self`
标识。

## 3.4 PD 成员配置与变更

新节点通过 `TASKITE_PD_CONSENSUS_BOOTSTRAP_PEERS` 发现现有集群后先注册为 learner。追平不会自动晋升；只有以下运维
请求会生成 `MEMBER_PROMOTE_VOTER`：

```bash
curl -X PATCH "$PD/configuration" \
  -H 'Content-Type: application/json' \
  -d '{"add":["meta-3"],"count":4}'
```

请求支持：

- `add`：将已注册且在线的 learner/spare 加入目标 voter roster。
- `remove`：完整移除指定 PD；移除 Leader 时先执行 leadership transfer。
- `count`：最终 voter 数；count-only 会确定性选择 learner 或非 Leader voter。

查询目标 roster 与 JRaft committed configuration：

```bash
curl "$PD/configuration"
```

响应区分 `target.voters`、`actual.voters`、`actual.learners` 和 `activeChangeId`。变更返回
`202` 与 `changeId`；目标已满足时返回 `200` 和 `NO_CHANGES`。

```bash
curl "$PD/changes/active"
curl "$PD/changes/$CHANGE_ID"
```

进度响应包含 `status`、source/target epoch、目标 voters、实时 voters/learners、
`currentOperation`、`plannedChanges` 和 `pendingChanges`。`plannedChanges` 始终保留最初
生成的完整 operation 列表；`pendingChanges` 只包含尚未执行的 operation，变更进入终态后
为空。原 `operations` 字段作为 `plannedChanges` 的兼容别名保留。本期不提供 PD change
cancel API。

## 3.5 集群生命周期与配置

### 创建集群

```bash
curl -X POST "$BROKER/clusters" \
  -H 'Content-Type: application/json' \
  -d '{
    "clusterId": "demo",
    "clusterSize": 3,
    "partitionSize": 5,
    "replicationFactor": 3
  }'
```

四个字段均为必填。首次创建返回 `201`；相同配置重复创建返回 `200` 且
`created=false`；同名不同配置返回 `409 CLUSTER_CONFIG_CONFLICT`。

配置约束：

- `clusterSize > 0`
- `partitionSize > 0`
- `0 < replicationFactor <= clusterSize`
- `partitionSize` 不得超过协议上限

### 查询配置

```bash
curl "$BROKER/clusters/$CLUSTER/configuration"
```

响应包含 `clusterId`、`clusterSize`、`partitionSize` 和 `replicationFactor`。

### 删除集群

```bash
curl -X DELETE "$BROKER/clusters/$CLUSTER"
```

仅当集群无 Broker，或所有 Broker 均为 `OFFLINE` 时允许删除。存在 `ONLINE` 或
`DRAINING` Broker 时返回 `409 CLUSTER_HAS_ACTIVE_BROKERS`。重复删除返回 `204`。

## 3.6 Broker 路由拓扑

```bash
curl "$BROKER/clusters/$CLUSTER/topology"
```

关键字段：

- `leaderId`：Broker 心跳中观测到的当前 Raft Leader
- `replicas`：除 Leader 外的 voter
- `learners`：非 voter 副本
- 顶层 `partitions`：PD committed state 计算出的路由与目标 membership 拓扑
- `brokers`：运行时可路由 Broker endpoint 及其最近一次注册/续约观测
- `brokers[*].partitions`：该 Broker 上报的本地分区 Raft 状态，包含 `role`、`term`、
  `lifecycleState`、`matchIndex`、`lag`、`readyForPromotion`、`priority`、`lastOpId`、
  `lastOpStatus` 和 `recoveryEpoch`

`brokers[*].partitions` 是按 Broker 组织的实时观测，用于判断选举和复制进度；顶层
`partitions` 是按分区组织的权威路由视图，两者不能互相替代。

新 placement 只选择 `ONLINE` Broker。已进入 `DRAINING` 的 Broker 在其 membership 或
leadership 引用完全迁移前仍保留 endpoint，以便完成 Leader 转移和 `LEAVE`；`OFFLINE`
Broker 不进入路由拓扑。

## 3.7 统一配置变更

Broker、分区数和副本因子统一通过一个 PATCH 接口修改：

```http
PATCH /api/v1/broker/clusters/{clusterId}/configuration
Content-Type: application/json
```

完整请求结构：

```json
{
  "brokers": {
    "add": ["broker-3"],
    "remove": ["broker-0"],
    "count": 3
  },
  "partitions": {
    "count": 8,
    "replicationFactor": 3
  }
}
```

所有字段均可选，但请求至少需要一个 action 字段。

### 添加或替换 Broker

待添加 Broker 必须已注册且为 `ONLINE`：

```bash
curl -X PATCH "$BROKER/clusters/$CLUSTER/configuration" \
  -H 'Content-Type: application/json' \
  -d '{
    "brokers": {
      "add": ["broker-3"],
      "remove": ["broker-0"]
    }
  }'
```

执行顺序遵循 learner-first：

```text
JOIN -> 等待追平 -> PROMOTE_LEARNER -> 调整优先级
     -> TRANSFER_LEADER（如需要）-> LEAVE
```

被移除 Broker 先进入 `DRAINING`，待所有分区不再引用后删除注册记录。本地 Raft group 在
对应分区完成 `LEAVE` 后关闭，日志中的 `DB destroyed` 表示 RocksDB/JRaft group 已关闭。

### 按数量扩缩 Broker

```bash
curl -X PATCH "$BROKER/clusters/$CLUSTER/configuration" \
  -H 'Content-Type: application/json' \
  -d '{"brokers":{"count":4}}'
```

只设置 `count` 时，PD 从已注册的 ONLINE Broker 中确定目标 roster。若同时提供
`add/remove/count`，执行 add/remove 后的 roster 数量必须等于 count。

### 增加分区

```bash
curl -X PATCH "$BROKER/clusters/$CLUSTER/configuration" \
  -H 'Content-Type: application/json' \
  -d '{"partitions":{"count":8}}'
```

分区数只允许增加，不允许减少；减少时返回
`409 PARTITION_SIZE_DECREASE_NOT_ALLOWED`。

### 修改副本因子

```bash
curl -X PATCH "$BROKER/clusters/$CLUSTER/configuration" \
  -H 'Content-Type: application/json' \
  -d '{"partitions":{"replicationFactor":3}}'
```

目标 topology 必须有足够 ONLINE Broker。降低 RF 时不得低于当前 quorum。

### PATCH 响应

无变化时返回：

```json
{"changeId":"NO_CHANGES"}
```

异步变更返回 `202` 和聚合 UUID：

```json
{"changeId":"7d98d3c2-6548-4d9b-a48a-7892d375d6b1"}
```

同一集群只允许一个在途变更。冲突时返回 `409 TOPOLOGY_CHANGE_IN_PROGRESS`；
`pendingChangeIds` 返回真实的 aggregate/member/partition changeId，可通过
`changes/{changeId}` 逐一查询阻塞计划，不使用 clusterId 占位。

## 3.8 显式 Rebalance

```bash
curl -X POST "$BROKER/clusters/$CLUSTER/rebalance"
```

该接口按当前配置和 ONLINE Broker 重算 deterministic placement。它是人工运维入口，不应
由业务请求或定时任务无条件调用。无变化时返回 `200`，创建计划时返回 `202`。

日常扩缩容优先使用统一 PATCH 接口。

## 3.9 删除与 Drain 语义

删除 Broker 不再使用单独的 Broker DELETE 接口，而是：

```bash
curl -X PATCH "$BROKER/clusters/$CLUSTER/configuration" \
  -H 'Content-Type: application/json' \
  -d '{"brokers":{"remove":["broker-3"]}}'
```

迁移期间：

- Broker 状态保持 `DRAINING`，续约不能将其恢复为 `ONLINE`
- DRAINING Broker 不参与新 placement
- DRAINING Broker endpoint 继续保留，直到 Leader 和 membership 引用迁移完成
- 最后一个分区完成后，PD 删除 Broker registration

## 3.10 Fenced Quorum Recovery

该接口仅用于分区永久失去 quorum 且故障节点已被带外隔离的场景：

```bash
curl -X POST \
  "$BROKER/clusters/$CLUSTER/partitions/3/quorum-recovery" \
  -H 'Content-Type: application/json' \
  -d '{
    "confirm": "true",
    "fencingProof": "ticket-1234",
    "survivor": "broker-1"
  }'
```

参数：

- `confirm`：必须为字符串 `"true"`
- `fencingProof`：非空隔离凭证
- `survivor`：可选；省略时 PD 选择 matchIndex 最高的在线 current voter

安全约束：

- quorum 尚未丢失时返回 `409 QUORUM_NOT_LOST`
- 这是唯一允许执行 `RESET_PEERS` 的入口
- reset 后仅保留 survivor voter，可能丢失未复制数据
- 恢复 RF 使用正常 `JOIN -> PROMOTE_LEARNER` 流程

## 3.11 查询和取消聚合变更

### 查询聚合进度

不传 `changeId` 时优先返回该集群的在途 aggregate/member change；若当前只有后台
reconciliation 产生的 partition plan，则返回按 partition/changeId 排序后的第一条 pending
plan。三类状态都不存在时返回 `404 ACTIVE_CHANGE_NOT_FOUND`：

```bash
curl "$BROKER/clusters/$CLUSTER/changes/active"
```

PATCH 返回的聚合 UUID 可通过资源路径精确查询：

```bash
CHANGE_ID=7d98d3c2-6548-4d9b-a48a-7892d375d6b1
curl "$BROKER/clusters/$CLUSTER/changes/$CHANGE_ID"
```

无需执行分区计划的配置变更或 rebalance 会在 `CONFIGURED` 应用时直接持久化为
`COMPLETED`，可通过 `changeId` 查询，也不会阻塞后续变更。旧版本遗留的空计划
`PENDING` 聚合记录在读取时按已完成处理，终态记录不会再被 active 索引返回。
该规则只针对 manifest 中没有任何分区计划 ID 的记录；若 manifest 声明了计划但计划
记录缺失，查询仍返回 manifest 的状态及冻结目标拓扑，并保留在途阻塞保护。

响应字段：

- `status`：持久化的 aggregate status
- `currentTopology`：查询时刻的 committed topology
- `plannedChanges`：所有分区最初规划的完整 operation 扁平列表，终态后仍保留
- `pendingChanges`：仅包含尚未进入终态的分区级实时进度
- `expectedTopology`：聚合 manifest 中冻结的变更目标拓扑；仅分区级变更从计划计算

`pendingChanges` 每项包含：

```json
{
  "partitionId": 4,
  "status": "WAITING_LEADER_CONVERGE",
  "nextOperationIndex": 3,
  "operationCount": 5,
  "currentOperation": {
    "opId": "...",
    "type": "TRANSFER_LEADER",
    "targetBroker": "broker-1",
    "priority": 0
  },
  "remainingOperations": [
    {
      "opId": "...",
      "type": "TRANSFER_LEADER",
      "targetBroker": "broker-1",
      "priority": 0
    }
  ],
  "startedAt": "2026-08-11T00:00:00Z",
  "deadline": "2026-08-11T00:05:00Z"
}
```

常见 status：

- `PENDING`
- `WAITING_READY`
- `WAITING_LEADER_CONVERGE`
- `COMPLETED`
- `FAILED`

完成后 `pendingChanges=[]`；`plannedChanges` 保留原始规划，不随执行进度清空。

### 取消 ChangePlan

```bash
curl -X DELETE "$BROKER/clusters/$CLUSTER/changes/$CHANGE_ID"
```

取消不会回滚已应用操作，只停止剩余操作并接受当前已提交 membership。取消参数使用 PATCH、
rebalance 或 recovery 响应返回的聚合 `changeId`。

取消通过单一 Raft command/event 原子作用于 aggregate manifest 下的全部分区计划。终态
operation 重复取消返回 `200`；取消成功返回 `202` 和 `status=CANCELLED`；不存在返回
`404 CHANGE_NOT_FOUND`。

## 3.12 任务订阅与定时订阅

这些 HTTP 接口供 Console 和管理客户端使用。普通任务创建/删除复用 CommandDistribution，
定时创建/取消复用 Timer 命令；校验由既有 Processor 执行，状态通过事件和 Applier 落地。
Console 的交互、类型联动校验和验收要求见 [任务订阅管理需求与验收](../taskite/subscription-management.zh-CN.md)。

### 创建普通任务订阅

```http
POST /api/v1/broker/job-subscriptions
Content-Type: application/json

{
  "clusterId": "demo",
  "tenantId": "<default>",
  "name": "payment-template",
  "jobType": "payment",
  "variables": "{\"currency\":\"CNY\"}",
  "retries": 3,
  "priority": 50
}
```

`clusterId` 必须已注册，`jobType` 非空白；省略/空白租户使用 `<default>`。
`name` 可选，`variables` 必须是 JSON 对象的源字符串，省略/空白按 `{}` 处理。
`retries` 默认 3 且为正 int32，`priority` 默认 50 且范围为 1–100。
相同集群、租户与 jobType 的重复创建返回已有 Key，不更新已有模板；旧订阅全部分区删除完成后允许重新创建。

返回 `202`：

```json
{"distributionKey":9007199254741052}
```

该 Key 是订阅的创建分发 Key。响应确认 PD 受理，Broker 分区 ACK 异步完成。
HTTP 响应字段为 `distributionKey`，Gateway gRPC 创建响应对应字段为 `key`。

### 查询普通订阅与分发

```bash
curl "$BROKER/job-subscriptions"
curl "$BROKER/job-subscriptions?clusterId=demo&jobType=payment"
curl "$BROKER/job-subscriptions/9007199254741052"
```

列表返回 `{"total":...,"subscriptions":[...]}`，支持 `jobType`、`distributionKey`、`clusterId`
可选精确筛选，不提供服务端分页。记录用 `clusterId` 表示当前所属集群，
`sourceClusterId` 表示源集群，`creationType` 为 `SUBSCRIPTION` 或 `MIRROR`。
详情包括模板、generation、lastRedistributedAt 和目标分区 ACK/pending。
普通订阅状态为 `DISTRIBUTING`、`COMPLETED`、`DELETING` 或 `DELETED`。
API 保留已删除分发记录；Console 的当前列表排除 `DELETED`。

重新下发使用 `POST /job-subscriptions/{distributionKey}/redistribute`，
body 为 `{"expectedGeneration":"当前轮次"}`。返回 `202`，保留原创建 Key 和模板，
继续查询原 Key 的详情确认新轮次 ACK；陈旧轮次或已删除记录会被拒绝。

### 创建独立定时订阅

```http
POST /api/v1/broker/timer-subscriptions
Content-Type: application/json

{
  "clusterId": "demo",
  "tenantId": "<default>",
  "name": "payment-every-30-seconds",
  "type": "CYCLE",
  "expression": "R/PT30S",
  "jobTypes": ["payment"],
  "variables": "{}"
}
```

`name`、`expression` 必须非空白，`jobTypes` 必须是非空、无重复的非空白字符串列表，
且每个类型在当前集群、租户下已有完成分发的普通任务订阅。已完成的 Mirror 订阅同样可以被该作用域的定时计划引用。
`variables` 为 JSON 对象的源字符串，省略/空白使用 `{}`。

|   `type`   |                       字面量示例                        |         语义          |
|------------|----------------------------------------------------|---------------------|
| `DURATION` | `PT30S`、`P1D`                                      | 正 ISO 8601 时长后执行一次  |
| `CYCLE`    | `R/PT30S`、`R5/PT1M`、`R2/2026-12-01T00:00:00Z/PT1H` | 无限或有限次循环            |
| `CRON`     | `0 */5 * * * *`                                    | Spring 5.3 六字段 Cron |

时长要求正值和毫秒精度。类型不匹配、无效表达式、无法计算下一次执行时间都会被拒绝。
仅接受所选类型的字面量定时表达式，持久化表达式和 PD 调度时区。
`variables` 作为任务数据传给每次创建的 Job。

返回 `201`：

```json
{"timerSubscriptionKey":9007199254741099,"dueDate":1793400000000}
```

Key 为新建独立定时订阅的 Key，`dueDate` 为首次计划执行的 Unix 毫秒时间戳。
每次创建都会新建计划，不按 jobType 去重；每次触发为每个 jobTypes 项创建一个 Job。
HTTP 的 `timerSubscriptionKey` 对应 Gateway gRPC 创建响应中的 `key`。

### 查询当前定时订阅

```bash
curl "$BROKER/timer-subscriptions"
```

返回 `{"total":...,"subscriptions":[...]}`。每项包含 `timerSubscriptionKey`、
`clusterId`、`tenantId`、`name`、`type`、`expression`、`jobTypes`、
`variables`、`timeZone`、`dueDate`、`repetitions`、`occurrence`。
这里的 `dueDate` 是下次计划执行时间，`repetitions` 是剩余计划次数（`-1` 表示无限），
`occurrence` 是当前执行序号。列表不提供服务端筛选/分页，Console 本地过滤和分页。
已结束或已取消的计划被移出当前调度状态，不提供历史定时任务列表。

### 删除普通任务订阅

```bash
curl -X DELETE \
  "$BROKER/job-subscriptions/9007199254741052?clusterId=demo&tenantId=%3Cdefault%3E"
```

path 必须使用原始创建 Key；query 中 `clusterId` 必须非空白，`tenantId` 省略/空白使用默认租户。
非正、非整数或超出 int64 范围的 Key 返回 `400`，不存在或作用域不匹配返回 `404`。
Mirror 行应回到源集群删除源订阅，不使用目标集群作用域删除源 Key。

返回 `202` 和 `{"distributionKey":...}`。响应 Key 标识删除分发计划，
不是原始创建 Key。删除异步作用于源集群和持久化的 Mirror 目标集群，
关联 Job、Incident 和变量也会清理；继续查询原始创建 Key 的详情观察 `DELETING`/`DELETED`。
重复的同一删除请求沿既有分发去重逻辑返回相同删除分发计划。

### 删除定时订阅

```bash
curl -X DELETE \
  "$BROKER/timer-subscriptions/9007199254741099?clusterId=demo&tenantId=%3Cdefault%3E"
```

Key 必须为正 int64，集群和租户必须匹配当前计划。成功返回 `200`：

```json
{"timerSubscriptionKey":9007199254741099}
```

取消事件应用后移除定时状态和到期索引，停止后续调度。已创建或已发出的 Job 不会撤回。
不存在、已结束、已取消或作用域不匹配返回 `404`。
删除 Mirror 关系只移除同步关系，不等同于上述两类订阅删除，也不撤回已分发的订阅。

上述 HTTP Key、`dueDate`、`occurrence` 是 Java int64。客户端应使用精确解析，
Console 将它们保留为十进制字符串，不能先按 JavaScript number 舍入后再构造后续请求。
结构/语义错误通常返回 `400`，缺少资源返回 `404`；业务状态冲突返回 `409`。
网络中断、未确认的 5xx 或成功响应契约异常不能证明写入失败，应查询当前列表/原始分发状态后再决定是否重试。
完整创建、查询、重新下发和删除模板见 [pd.http](../apihub/placement-driver/pd.http)。

## 相关实现

- Controller：`src/main/java/io/taskite/placementdriver/api/rest/PlacementDriverAdminController.java`
- API 路由：`src/main/java/io/taskite/placementdriver/api/PlacementDriverEndpointManager.java`
- Processor：`src/main/java/io/taskite/placementdriver/processing/`
- 请求 DTO：`src/main/java/io/taskite/placementdriver/api/model/PlacementDriverHttpRequestModel.java`
- 响应 DTO：`src/main/java/io/taskite/placementdriver/api/model/PlacementDriverHttpModel.java`
- Console 订阅需求与验收：[任务订阅管理](../taskite/subscription-management.zh-CN.md)

