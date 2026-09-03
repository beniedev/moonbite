# Moonbite

> **Let AI persist in its own way.**
>
> **让 AI 以自己的方式持续存在。**

**Moonbite 让 AI Agent 能够跨会话、跨小时甚至跨天保持连续性。**

它为长期运行的 Agent 提供跨会话记忆、短期工作状态、有限自主活动，以及
“什么时候该行动、什么时候该保持安静”的判断机制。Moonbite 还会记录实际
发生的外部操作，而不是把模型声称“已经做了”当成执行成功。

Moonbite 不取代宿主 Agent。模型、工具、凭据、调度和消息投递仍然由宿主
负责。

Moonbite 是一项实验性作品：首要目标是传达设计理念，并探索这些理念在
实践中的可能性。

[![CI](https://github.com/beniedev/moonbite/actions/workflows/ci.yml/badge.svg)](https://github.com/beniedev/moonbite/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python: 3.11–3.14](https://img.shields.io/badge/Python-3.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue.svg)](pyproject.toml)
[![Platform: Linux / WSL2 / macOS](https://img.shields.io/badge/Platform-Linux%20%7C%20WSL2%20%7C%20macOS-informational.svg)](COMPATIBILITY.md)
[![Status: Public Preview](https://img.shields.io/badge/Status-Public%20Preview-orange.svg)](#项目状态)

[English](README.md)

## 公开预览范围

本次预览支持：

- Hermes Agent 是当前唯一正式支持的宿主
- 锁定不可变 Commit SHA 的源码安装
- Linux、使用原生 Linux 文件系统的 WSL2，以及 macOS
- Runtime Core、Heartbeat、Autonomy、Panel / Daily RAM、Memory /
  Diary、运行时控制、可验证的外部操作结果与审计记录
- 由宿主负责的模型路由、凭据、调度、网络访问、Gateway 执行与消息投递

本次预览不提供：

- 发布到 PyPI 或其他软件包仓库
- 稳定的公开 Python API 保证
- 非 Hermes Agent 宿主框架支持
- 生产支持或长期兼容性保证
- 内置调度器、凭据存储、网络浏览器或消息渠道

Moonbite 尚未发布到软件包仓库。预览版本仅以源码提供；请通过 Hermes
使用 GitHub Release 标签安装，或锁定对应的不可变 Commit SHA。

## 为什么长期运行的 Agent 需要 Moonbite

多数 Agent 系统只负责回答一次提示，或完成一个会话。长期运行的 Agent
可能需要持续陪伴一个人、观察一套系统，或跟进一个不断变化的队列。为了
做好这些工作，它必须记得过去发生了什么、知道现在什么最重要、判断一件
事是否值得行动，并确认请求的操作是否真的发生。

Moonbite 提供这些连续性能力，但不会成为另一个 Agent，也不会取代宿主。
主 Agent 仍然负责理解信息并做最终判断；宿主仍然负责执行和投递。

## Moonbite 如何工作

```text
            Main Agent
                ▲
                │
┌────────── Moonbite ──────────┐
│                              │
│  Memory       Panel          │
│  remembers    knows now      │
│                              │
│  Heartbeat    Autonomy       │
│  when to act  what to do     │
│                              │
└──────────────┬───────────────┘
               │
               ▼
         Hermes / Host
  models · tools · schedule · delivery
```

**Moonbite 到底干嘛？**

→ 给长期运行的 agent 加上：

**记得过去 / 知道现在 / 判断何时行动 / 执行有限自主行为**

Hermes 则继续管模型、工具、调度和投递。

主 Agent 仍然负责最终理解和判断。

## 核心组件

- **Memory / Diary** 记住跨会话仍然重要的内容。检索结果会指向可以打开
  核对的准确证据；维护操作会保留来源与历史。
- **Panel / Daily RAM** 保存“现在什么最重要”的有界短期状态，包括每日
  滚动和已经验证的活动余韵（Afterglow）。
- **Heartbeat** 判断一件事现在是否值得关注、行动或升级。它是判断流程，
  不是定时器。
- **Autonomy** 在宿主提供机会时，每次 Tick 最多执行一项符合条件的有限
  自主活动。如果选中的活动失败，Moonbite 会记录失败，不会在同一次 Tick
  改选另一项。
- **Runtime Core** 用追加式记录维护事件、状态变化、控制规则与决策，使它们
  保持一致并可供审计。
- **控制规则与执行回执** 负责暂停、配额、频率、执行条件和安全限制。只有
  收到匹配的宿主执行回执（receipt），外部操作才会被认定为已接受或完成。

## Moonbite 与宿主各自负责什么

Moonbite 不包含调度器、后台服务、凭据存储、网络浏览器、模型路由器或消息
渠道。这些能力以及原始会话历史与搜索都由宿主负责。Hermes 宿主适配器
（host adapter）精确注册 10 个工具与 7 个生命周期 Hooks；完整接口见
[SETUP.md](SETUP.md) 与 [plugin.yaml](plugin.yaml)。

七个 Hooks 分别是 `pre_gateway_dispatch`、`on_session_start`、
`pre_llm_call`、`post_llm_call`、`on_session_end`、`on_session_finalize` 与
`subagent_stop`。

默认配置下：

- 仅启用 `runtime_core`；
- `heartbeat`、`autonomy`、`panel` 和 `memory` 均关闭；
- 消息投递使用 `noop` 适配器；
- 模型路由未配置；
- 仅安装 Moonbite 不会启动后台任务、调用模型、发起网络请求或发送外部消息。

任何可见的外部操作都需要匹配的宿主执行回执。禁用或卸载 Moonbite 不会
删除其状态目录。

## 目前仅支持 Hermes 安装

请以禁用状态安装经过审核的完整 40 位 Commit SHA：

```bash
hermes plugins install beniedev/moonbite \
  --ref "<40-character-commit-sha>" \
  --no-enable
```

随后审查安装器的安全检查结果、运行插件清单检查、合并经维护者审核的配置，
并在准备完成后启用：

```bash
hermes plugins doctor moonbite --ci
hermes plugins enable moonbite --no-allow-tool-override
```

不要把浮动的 `main` 分支当作稳定目标安装。完整的隔离安装、授权、重启与
回滚流程见 [SETUP.md](SETUP.md)。

## 验证、Doctor 与回滚

启动新的 Hermes 进程后运行：

```bash
hermes moonbite doctor
hermes moonbite status
```

Doctor 不会产生外部影响：不调用模型、不探测网络，也不写入状态。安全的默认
结果应包含 `ok: true`、`network_probe: "not_performed"`、
`writes_performed: false` 与 `delivery_adapter: "noop"`。

要停止加载代码，运行 `hermes plugins disable moonbite`；要移除已安装的
代码副本，运行 `hermes plugins remove moonbite`。两者都不会删除 Moonbite
状态；留存策略与物理删除仍由宿主负责。

## 实验性接口

仓库包含一套不绑定特定宿主的实验性 Panel API，用于设计探索和未来的宿主
适配器。它已经过测试，但不属于初始的“仅支持 Hermes”契约，也不提供稳定
API 保证。

详见 [docs/features/PANEL.md](docs/features/PANEL.md)。

## 设计理念与技术文档

- [设计理念（简体中文）](docs/DESIGN_PHILOSOPHY.zh-CN.md) 解释 Moonbite
  为什么存在。
- [Design philosophy](docs/DESIGN_PHILOSOPHY.md) 是英文版本。
- [DESIGN.md](DESIGN.md) 定义架构与安全边界。
- [CONFIGURATION.md](CONFIGURATION.md) 说明配置与预设。
- [COMPATIBILITY.md](COMPATIBILITY.md) 记录已测试的平台与 Hermes 契约。
- [DEPLOYMENT_COMPATIBILITY.md](DEPLOYMENT_COMPATIBILITY.md) 定义既有部署的
  迁移要求。
- [SECURITY.md](SECURITY.md) 说明漏洞报告与发布隐私门。
- [CHANGELOG.md](CHANGELOG.md) 记录尚未发布的项目变更。

## 项目状态

```text
Status: 0.1.0 Alpha 1 public preview
Supported host: Hermes Agent only
Distribution: source-only GitHub prerelease
Support: best effort
API stability: not guaranteed
```

本次预览会尽早公开设计、运行时契约与可工作的实现。随着项目从谨慎的
真实使用中学习，接口与兼容性仍可能变化。

## 开源协议

Moonbite 基于 [MIT License](LICENSE) 开源。
