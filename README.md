# Paily Fetch — Paily ON NAS

> **Paily ON NAS 自托管平台的抓取服务。**
> 从主仓 `paily-core` 获取来源，下载订阅、解析内容、校验节点后把有效节点上报。

本仓库是 [Paily ON NAS](https://github.com/Eiyuuzaki/Paily-ON-NAS) 的抓取组件，基于 Paily Connect 已开源的 `paily-fetch` 继续维护（AGPL-3.0）。本分支基本沿用上游，仅供个人学习自用。

![Platform](https://img.shields.io/badge/platform-linux%2Farm64-blue) ![Go](https://img.shields.io/badge/Go-1.24%2B-00ADD8) ![License](https://img.shields.io/badge/license-AGPL--3.0-orange)

## 在 Paily ON NAS 中的位置

```text
   订阅源 / 散源
        │
        ▼
   Paily 抓取 (paily-fetch)      下载 · 识别 · 解析 · 校验
        │
        ▼
   Paily 主服务 (paily-core)     源库 · 节点池 · 去重 · 分发
```

用法与原仓库一致，见下方原仓库 README。

---

## 原仓库 README（Paily Connect 上游 `paily-fetch`）


> [!IMPORTANT]
> **本仓库为 Paily Connect 开源仓库的一部分**
>
> 关于 Paily Connect 开源仓库的更多信息，请查看 [此页面](https://github.com/openpaily/paily-core) 。
> 
> Paily Connect 系列是我们的内部项目，其仍处于早期开发阶段。
> **我们极度不建议您直接部署此版本**。该版本可能会有潜在的未知问题并可能造成数据丢失等异常，我们建议您等待可用的分支版本部署。

Paily Fetch 是 Paily Connect 平台的抓取服务。它从 **Paily 主服务**（`paily-core`）获取来源，下载订阅、解析内容、校验节点，然后将有效节点上报。

## 在平台中的位置

```text
  订阅源 / 散源
        │
        ▼
   Paily Fetch          下载 · 识别 · 解析 · 校验
        │
        ▼
   Paily 主服务          源库 · 节点池 · 去重
        │
        ▼
   Paily 测活后端        初筛 · 复筛
        │
        ▼
   Paily 主服务          评分 · 分发
```

## 工作流程

每个抓取轮次按以下顺序进行：

1. 向 Paily 主服务请求所有启用中的来源。
2. 按来源类型分流：订阅源进入下载队列，散源直接进入解析队列。
3. 下载订阅内容，并识别订阅的格式。
4. 解析为统一节点表示，并进行格式校验，丢弃无效节点。
5. 汇总每个来源的抓取结果并上报，包括成功状态、节点数量和有效节点。

启动时会立即执行一轮，之后按 `FETCH_INTERVAL` 周期执行。抓取失败会在下一轮重新尝试；同一来源的多条地址顺序下载，不同来源之间并发处理。

## 运行

```sh
export PAILY_SERVICE_SECRET=replace-with-your-service-secret
export PAILY_CORE_URL=http://localhost:8080

go run ./cmd/fetcher
```

## 配置

配置全部通过环境变量提供。

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PAILY_CORE_URL` | `http://localhost:8080` | Paily 主服务 API 地址。 |
| `PAILY_SERVICE_SECRET` | 必填 | 服务密钥，作为 Bearer 凭据发送给 Paily 主服务。 |
| `FETCH_INTERVAL` | `600` | 抓取轮次间隔，单位为秒。 |
| `FETCH_CONCURRENCY` | `10` | 并发下载订阅的 Worker 数。 |
| `FETCH_PARSE_CONCURRENCY` | `NumCPU` | 并发解析 Worker 数，属计算密集型，建议保留默认值。 |
| `DOWNLOAD_TIMEOUT` | `30` | 单次下载超时，单位为秒。 |
| `FETCH_UA` | `clash-verge/v1.0.0` | 订阅下载请求的 User-Agent。 |

## 输入格式

Paily Fetch 能识别多种订阅内容与代理协议，并支持订阅源中的动态规则。详见 [输入格式](docs/input-formats.md)。

订阅源内容本身的语法属于本服务的解析规则，源库中如何维护来源则见 Paily 主服务的 `docs/source-management.md`。

## Docker

```sh
export PAILY_SERVICE_SECRET=replace-with-your-service-secret
export PAILY_CORE_URL=http://paily-core:8080
docker build -t paily-fetch .
docker run -d --name paily-fetch -e PAILY_CORE_URL -e PAILY_SERVICE_SECRET paily-fetch
```

## 许可证

本项目基于 GNU Affero General Public License v3.0（AGPL-3.0）发布，详见 [LICENSE](LICENSE)。
