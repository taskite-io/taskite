# Taskite Console

PD HTTP 服务内的集群管理后台。采用 `client` + `server` 组织方式：React 页面直接访问现有 `/api/v1`；Spring MVC 提供页面、静态资源和展示配置，不参与 PD 状态读写。

## 使用

启动启用 `placement-driver` profile 的 Taskite 发行包后访问：

- 页面：`http://<PD HTTP 地址>/console/`
- 展示配置：`GET /api/v1/console/bootstrap`
- 第三方许可：`/console/licenses.txt`

Console 默认启用，与 PD 共用 HTTP 端口（默认 19001）。新增模块不改变 HTTP、gRPC 或 Raft 默认端口。bootstrap 不返回 Grafana 信息。

```yaml
taskite:
  console:
    enabled: true
    deployment-id: staging-cluster-a
```

发行启动配置支持 `TASKITE_CONSOLE_ENABLED`、`TASKITE_CONSOLE_DEPLOYMENT_ID` 环境变量。关闭 Console 或使用非 PD profile 时不装配展示路由。

`deployment-id` 用于环境显示与浏览器记录隔离；建议在测试集群重建时更新。Console 的写请求提交至当前 HTTP 节点，由 PD 通过既有 gRPC 路由转发给 Leader。Console 和 `/api/v1` 应共同置于已有管理网络或访问代理保护范围；使用 Cookie 登录的代理须同时管理写权限及 CSRF。代理保留 `/console/`、`/api/v1` 前缀。

## 本地开发

要求 Java 21、Maven，以及 Node >=22.12。Maven 固定下载 Node 22.22.0 和 npm 10.9.2，npm 依赖由 `package-lock.json` 锁定。

```bash
# 从仓库根目录构建页面、资源 JAR 与服务端
mvn -pl webapp/server -am package -DskipTests

# 从 webapp/client 目录启动开发服务
npm ci
TASKITE_CONSOLE_DEV_PROXY_PD_HTTP_URL=http://127.0.0.1:19001 npm run dev
```

开发入口 `/console/`，`/api/v1` 由 Vite 代理到 `TASKITE_CONSOLE_DEV_PROXY_PD_HTTP_URL`。HTTP 地址应使用当前可访问的 PD 管理入口；Console 本身不实现多节点故障转移。

Grafana 独立使用前端构建环境变量 `VITE_TASKITE_CONSOLE_GRAFANA_URL`（可放入 `client/.env.local`）。配置 HTTP(S) URL 后在顶部显示链接，未配置时不显示。改变该配置需要重新构建页面。

沿用传入应用的 HashRouter，深链示例为 `/console/#/clusters/demo?tab=partitions`、`/console/#/workers?cluster=demo`。所有页面刷新均从 `/console/` 装载资源，业务请求仍为同源 `/api/v1`。

## 功能与数据范围

- PD：health、成员拓扑、目标/实际配置、成员增删或数量调整、当前与指定 ID 变更。提升状态仅适用于 Learner → Voter：实际 Voter 显示“无需提升”，Learner 根据当前成员变更显示“等待提升”“追平中”或“提升中”；不将心跳中默认的 readyForPromotion 当作实时判定。
- Broker：集群创建/删除、注册与副本报告、分区详情、组合配置、Drain、Rebalance、人工 Quorum Recovery、变更取消。详情保留概览、Broker、分区三个页签；页头“修改配置”打开右侧抽屉，预览后确认提交，返回编辑保留草稿。Broker 列表固定关键列宽，端口与数值保持单行；分区矩阵与图例共用蓝色系角色标签并适配亮/暗主题。
- Worker：Name、Cluster、Tenant 表头提供服务端精确查询与重置，空结果保留表头；筛选及所选 Worker 进入 URL。多租户注册详情和消费配置来自心跳快照。
- 任务订阅：普通任务/定时任务两个 Tab，各自提供列表、筛选、详情、创建与删除；创建入口随当前 Tab 切换。普通任务展示原生/Mirror 来源及分区 ACK，定时任务展示当前调度计划与关联任务类型；Mirror 关系另有列表/筛选/创建/查询/删除。
- 追踪：按作用域查询当前变更、已知 ID 查询、最多 50 条按 origin + deploymentId 隔离的浏览器访问记录。

集群信息的标签和值同行显示。Broker 配置抽屉顶部展示当前配置，数值使用蓝色胶囊；列表 Raft 端口和分区数使用同系列胶囊。分区输入低于当前配置时，失焦或按回车会报错并恢复为当前分区数；配置没有实际差异时禁用预览/提交，确认期间也随最新配置重新判定。分区矩阵固定列使用不透明行底色，横向滚动和 hover 时不透出后方色块。

PD 与 Broker 页头入口均为“修改配置”，使用一致的主色实心按钮；PD 抽屉和确认标题同步采用配置文案。集群列表 Rebalance 使用浅蓝底、蓝色边框与图标，亮暗主题沿用产品色调。

添加 Broker 通过下拉多选，只提供在线且所有已报告分区均为 Learner 的节点，并排除拓扑中已有的 Leader/Voter；不支持手动输入新 ID。候选为空时显示说明，所选节点失效或读取快照过时时禁用预览/确认，提交前重新读取拓扑校验。

创建 Mirror 的源、目标均从当前已有集群中下拉选择，可搜索，不支持自由输入集群 ID；两侧互相排除已选项。集群不足两个时显示提示，列表读取失败时可刷新；所选集群删除后清空选择，提交前再次检查两个集群仍存在且不同。

任务订阅入口为 `/console/#/subscriptions/distributions`，定时 Tab 使用 `?tab=timer`，刷新保留当前 Tab。两类创建表单都从已有集群中选择作用域，租户留空使用 `<default>`；变量使用 JSON 代码编辑器，支持高亮、行号和格式化，并保留长整数精度。创建成功直接关闭弹窗、刷新当前列表，不跳转详情或展示成功结果框。

定时任务仅能选择当前集群、租户下已完成分发的任务类型；切换集群或租户清空选择。Duration、Cycle、Cron 仅接受字面量定时表达式，随输入和类型变化重新校验，Duration 填写 `R/PT30S` 会立即报错。PD 校验并持久化表达式和调度时区，任务变量传给每次创建的 Job。

两个列表均提供删除确认。普通任务的删除异步分发到源及 Mirror 目标集群，在途显示 `DELETING`，完成后从列表隐藏；Mirror 行提示在源集群删除。定时删除停止后续调度，不撤回已创建或已发出的 Job，成功后刷新列表。完整字段约束、API 约定和回归清单见 [任务订阅管理需求与验收](../taskite/subscription-management.zh-CN.md)。

PD 变更详情将 ID 与配置 Epoch 演进收进紧凑信息栏，目标配置和实际成员状态使用对称卡片；当前操作独立展示，操作计划统一中文表头。原始目标与实时状态分别更新，已结束且没有当前操作时收起相应卡片；空计划、未知操作、长 ID 与错误信息保留明确显示。窄屏按顺序堆叠，操作表格在容器内横向滚动。

long 字段先使用 lossless JSON 解析，再按端点契约保留十进制字符串；订阅 Key、定时时间戳和执行序号同样保持精确。未知枚举保留原文。204 无正文不解析，202 表示受理，`NO_CHANGES` 不建立追踪。页面使用可取消轮询，退出页面/更换筛选会终止旧读取；查询可按终态停止，订阅列表和分发详情持续轮询。明确 400/403/404 停止轮询，其他读取失败按周期恢复。写请求不自动重试，网络中断或成功响应契约异常显示“结果未知”，需查询实际状态；订阅创建/删除未确认的 5xx 也会阻止直接重复提交。

页面切换不使用整页透明度动画。当前环境内保留最多 80 个、有效期 2 分钟的内存查询快照，查询键包含完整作用域、资源 ID 和筛选参数；返回页面立即展示快照并重新请求，刷新失败时提示过时。明确 400/403/404 清除对应快照。快照不写入浏览器存储，切换 deploymentId 或重新加载后清空。

Worker 数据来自注册心跳，多租户行不汇总为全局 Job 数；离线行保留最后快照。现有 API 支持全量任务订阅分发列表和当前定时订阅列表，筛选/分页主要在前端完成；不支持 Worker 配置修改、消费历史、Job 列表、已结束定时任务历史或全量变更历史。

分区副本来自 placement 和 Broker 报告的并集，缺失报告保持未知。lag、Leader ID、分区数均不作为健康或资源可用的完成判断；资源 ACK 使用订阅创建 Key 独立查询。人工恢复必须提供旧副本隔离凭证并精确确认作用域，发送 `confirm: "true"`，最终前置条件由 PD 检查。取消不回滚已执行操作；集群删除不停止进程、不删除物理目录。PD 删除集群时清理该集群的订阅、定时计划、Mirror 关系及相关管理状态；若仍是其他集群订阅的持久化分发目标，删除会被拒绝。

## 验证

```bash
# webapp/client：浏览器契约与交互 E2E（首次需安装 Chromium）
npx playwright install chromium
npm run build
npm test

# 仓库根目录：真实 HTTP 集成与模块 verify
mvn -pl webapp/server -am verify

# 发行包：相关回归与打包
mvn -pl dist -am package -Dfrontend.skip=true -DskipTests
```

`frontend.skip=true` 只用于已构建本地前端时复用产物，不代替 CI 的完整构建。E2E 使用生产页面与可控 HTTP fixture，HTTP 集成启动真实 Spring Servlet 服务验证资源/路由/profile/开关；订阅管理的实际验证范围和复跑命令见 [需求与验收](../taskite/subscription-management.zh-CN.md#验收矩阵)。真实集群上的删除等操作仅在隔离环境验证。

真实 PD 的浏览器集成验收单独运行，不使用 HTTP mock；要求目标 bootstrap 的 deploymentId 以 `console-smoke-` 开头：

```bash
# 在 webapp/client 目录运行；先启动独立端口、独立数据目录的 PD
PD_CONSOLE_URL=http://127.0.0.1:19191 npx playwright test --config playwright.live.config.ts
```

该场景创建两个独立测试集群和一条 Mirror，验证 Hash 深链、创建/删除、无 Broker 注册空态、PD/Worker 读取，并清理本次创建的配置。截图输出到 `client/target/live-console-overview.png`。无 Broker 注册时 topology 的 `NO_BROKER_REGISTERED` 是等待注册状态，不构造假拓扑；删除前额外确认没有在线 Broker，由后端继续校验实际注册状态。

## 许可

Taskite 源代码沿用仓库根目录的 [Taskite Community License](../LICENSE)。前端生产包由 Vite 生成 `licenses.txt`，随资源 JAR 和发行包一同分发，包含实际打入 bundle 的第三方许可信息。直接依赖登记见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)，完整版本和传递依赖以 lockfile 为准。
