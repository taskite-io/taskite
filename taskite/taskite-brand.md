# Taskite 品牌定位

## 产品

Taskite 是面向普通任务与定时任务的分布式任务调度引擎。它负责持久化任务状态、分布式任务分发、Worker 注册与租约、周期调度、故障恢复和水平扩展。

> Taskite is a distributed task scheduling engine built for durable, fault-tolerant execution at scale.

Gateway 提供任务接口并路由请求；Placement Driver 维护拓扑、注册租约和任务订阅；Broker 使用 Multi-Raft 分区保存任务状态；Worker 激活并执行任务。

品牌表达围绕分布式任务调度与可靠执行。

## 核心价值

- **可靠执行**：持久化状态与复制日志支持任务处理和故障后的恢复。
- **分布式扩展**：Broker 分区和 Worker 分散负载，Gateway 可以水平扩展。
- **普通与定时任务**：直接创建任务，或通过显式到期时间、间隔及执行次数维护任务订阅。
- **开发者接口**：使用结构化 Protocol Buffers 和 gRPC 接入任务与 Worker。
- **可观测性**：通过任务状态、Incident、运行指标和 Grafana 看板观察执行与集群运行。

## Slogan

> **Distributed tasks. Reliable execution.**
>
> **分布式任务，可靠执行。**

这句话同时说明产品类别与核心价值，适合产品首页、代码仓库、Logo 和启动 Banner。

其他候选见 [logo/README.md](logo/README.md)。

## 视觉标识

母版为 [taskite-icon.svg](taskite-icon.svg)。中央 T 对应 Taskite；环形轨道连接三个节点，表达任务分发、周期调度和故障后的持续执行。三个节点表示分布式执行，不限定部署节点数量。

主色为蓝色 `#4657E8`，辅以轨道蓝 `#3478ED` 和青绿色 `#10AAA9`。深色 Logo 提高图案亮度；CLI 单色图标继承宿主的 `currentColor`。

Logo、应用图标和 GitHub 组织头像使用同一组图形元素。Spring Boot Banner 使用 Taskite ASCII 字标；所有资源使用统一的产品 Slogan。

## 标准信息

- 产品名：**Taskite**
- 中文定位：**分布式任务调度引擎**
- 英文定位：**Distributed Task Scheduling Engine**
- 官网：[taskite.io](https://taskite.io)
- 仓库：[github.com/taskite-io/taskite](https://github.com/taskite-io/taskite)
- Java 命名空间：`io.taskite`
- CLI：`tkctl`
- 许可：Taskite Community License 1.0

文件、尺寸与使用规则见 [品牌资产说明](brand-assets.md)。
