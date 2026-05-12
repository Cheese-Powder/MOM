# 分布式日志采集、分析与异常检测（MOM 作业）

基于 **RabbitMQ** 消息中间件：多进程模拟多个采集节点、独立分析微服务、Spring Boot 可视化与 WebSocket 实时推送。

## 环境

- JDK 21、Maven
- [RabbitMQ](https://www.rabbitmq.com/)（默认 `localhost:5672`，用户/密码 `guest`/`guest`）

### 启动 RabbitMQ（Docker 示例）

```bash
docker run -d --hostname mom-rabbit --name mom-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

## 配置参数（分析服务）

| 含义            | JVM 参数                             | 默认  |
| ------------- | ---------------------------------- | --- |
| 每设备保留最近日志条数 N | `-Dmom.window.n=200`               | 200 |
| 分析结果上报周期 T（秒） | `-Dmom.report.period.seconds.t=5`  | 5   |
| 严重告警滑动窗口 S（秒） | `-Dmom.severe.window.seconds.s=30` | 30  |

RabbitMQ 地址：`RABBITMQ_HOST`、`RABBITMQ_PORT`。

## 运行步骤（四个终端）

在项目根目录执行（若本机 Maven 仓库路径异常，可加 `-Dmaven.repo.local=%TEMP%\mom-m2-repo`）。

### 1. 可视化监控（Web）

```bash
mvn spring-boot:run "-Dmaven.repo.local=%TEMP%\mom-m2-repo"
```

浏览器打开：http://localhost:8080/

### 2. 异常分析服务

```bash
mvn compile exec:java "-Dmaven.repo.local=%TEMP%\mom-m2-repo" -Dexec.mainClass=org.example.mom.analyzer.AnomalyDetectionService
```

自定义参数示例：

```bash
mvn compile exec:java "-Dmaven.repo.local=%TEMP%\mom-m2-repo" -Dexec.mainClass=org.example.mom.analyzer.AnomalyDetectionService -Dmom.window.n=100 -Dmom.report.period.seconds.t=3 -Dmom.severe.window.seconds.s=20
```

（`exec:java` 的 JVM 参数需通过 `MAVEN_OPTS` 或 `argLine` 传递时，可改用 `java -cp` 方式运行 `target/classes` 与依赖。）

更简单的自定义方式：在 IDE 中运行 `AnomalyDetectionService`，在 VM options 里加入 `-Dmom.window.n=...` 等。

### 3. 多个采集节点（多进程）

```bash
mvn compile exec:java "-Dmaven.repo.local=%TEMP%\mom-m2-repo" -Dexec.mainClass=org.example.mom.collector.LogCollectorNode -Dexec.args="node-A"
```

另开终端将 `node-A` 换成 `node-B`、`node-C` 等，即模拟多个带唯一 ID 的采集进程。每个节点约 **100ms** 产生一条 JSON 日志并发到 **`mom.logs.fanout`**。

采集节点约每 **25 秒** 模拟一次短时故障（大量 ERROR），便于在默认 **S** 窗口内触发 **ERROR 占比 > 50%** 的严重告警。

## 消息拓扑

| Exchange（fanout）      | 说明                    |
| --------------------- | --------------------- |
| `mom.logs.fanout`     | 采集节点 → 分析服务与仪表盘（原始日志） |
| `mom.analysis.fanout` | 分析服务 → 仪表盘（周期统计）      |
| `mom.alerts.fanout`   | 分析服务 → 仪表盘（严重告警）      |

## 作业打包提示

按课程要求将源码与设计报告打包为：`第2次作业+学号+姓名.zip`，在截止日期前上传智课平台。
