# K-TradingAgents-CN 本地部署与维护

部署目标：WSL2 Ubuntu 24.04 + Docker Desktop WSL 集成。

源码：`https://github.com/KI9I9/K-TradingAgents-CN`，基线分支 `v3.0`，提交 `51060a7682cf51810b4bc3a704829438d18cd8dc`。部署与迁移分支为 `deploy/ubuntu-migration-20261014`，包含本地部署文件和调度、助手提示修复。

## 目录与访问

在 PowerShell 执行 `wsl -d Ubuntu`，进入 Ubuntu 后：

```bash
cd /home/marvin/project/K-TradingAgents-CN
./deploy/ta.sh ps
cat runtime/initial-login.txt
```

浏览器访问 http://127.0.0.1:8088。初始管理员为 `admin`，随机密码仅存放在本地 `.env` 和 `runtime/initial-login.txt`，请登录后修改密码。修改页面登录密码不会自动改写这两个初始记录。本机 Windows 侧测试中，`localhost` 名称访问曾超时，明确使用 `127.0.0.1` 可正常访问。

仅前端发布 `127.0.0.1:8088`；后端、MongoDB、Redis 不发布主机端口。数据库及应用文件使用 `k-tradingagents` 项目专属命名卷。

Windows 资源管理器中的源码路径为 `\\wsl.localhost\Ubuntu\home\marvin\project\K-TradingAgents-CN`。当前 Ubuntu 的虚拟磁盘位于 `D:\wsl\wsl-ubuntu24.04`；Docker 的镜像和命名卷另存于 `C:\Users\pxq\AppData\Local\Docker\wsl\disk\docker_data.vhdx`。不要直接编辑或复制正在运行的虚拟磁盘来代替应用备份。

## 首次使用

1. 在系统设置中添加模型供应商、API Key 与实际可用的模型，先测试连接。
2. 配置数据源。可从 AKShare 开始；Tushare 需要自己的 Token 及对应权限。
3. 在数据同步页面先同步股票基础信息，确认股票列表和所选股票数据有效。
4. 选一只股票、低研究深度，验证一次完整研究流程。
5. 按实际需求启用数据同步计划；安装初始配置关闭周期性批量同步，避免未配置数据源时持续报错。
6. AI 记忆初始关闭。需要时先配置 Embedding 和相应模型，再将 `.env` 的 `MEM0_ENABLED`、`MEMORY_ENABLED` 改为 `true` 并重新创建后端。

API Key 不需要发给他人或写入 Git。这里的社区版 README 将定时分析、批量分析列为 Pro 功能，全天运行不等于解锁这些功能。

初始化会导入预置模型目录；就绪页面显示模型条目或“已就绪”，不代表供应商密钥已配置或实际推理已验证。首次安装时未填入模型 API Key；实际模型配置由使用者保存到数据库，迁移时应保留。应用启动时仍会进行一次基础信息补齐和知识索引初始化；未配置 Embedding 的提示不影响登录。

## 启停与日志

```bash
cd /home/marvin/project/K-TradingAgents-CN
./deploy/ta.sh up -d
./deploy/ta.sh ps
./deploy/ta.sh logs --tail=100 backend
./deploy/ta.sh stop
./deploy/ta.sh start
```

修改 `.env` 后执行 `./deploy/ta.sh up -d --force-recreate backend`，单纯 `restart` 不会更新容器环境变量。

修改源码后执行：

```bash
./deploy/ta.sh build backend frontend
./deploy/ta.sh up -d
```

`./deploy/ta.sh down` 会移除容器和网络、保留命名卷；**不要使用 `down -v`，除非明确要删除数据库及应用文件**。

数据库初始化密码只对空数据卷生效，不能仅修改 `.env` 就完成已有数据库密码轮换。

## 当前按需运行，未来再迁移服务器

当前需求为本地按需使用，运行后用 `./deploy/ta.sh stop` 停止，下次用 `./deploy/ta.sh start` 启动。不要为这次本地安装修改电脑睡眠或登录自启设置。

以下是将来评估持续运行时的说明：

Docker Desktop 需要保持运行；Windows 不能休眠、关机或重启。`restart: unless-stopped` 仅在 Docker 引擎运行时恢复容器，不会唤醒电脑。

需要时在 Windows 电源设置中将接通电源时自动睡眠设为“从不”，并在 Docker Desktop 中启用登录后自动启动。无人登录也要自动恢复的场景建议迁移远程 Ubuntu。

2026-10-13 的只读检查显示：Windows 当前接通电源后闲置 60 分钟会睡眠，电池模式为 15 分钟，Docker Desktop 登录自启为关闭。本次没有改动这些系统设置，因此安装完成后仍需按上面的说明设置，才能避免闲置时服务停止。

## 迁移到远程 Ubuntu

1. 保存当前 Git 修改、代码提交号、Compose 配置和实际构建版本；不要把 `.env` 提交到仓库。
2. 停止新分析任务并停止后端，保证数据库和应用文件备份的一致性。
3. 使用 `mongodump` 备份 MongoDB；备份 `.env`、应用 data/config/runtime/results/reports/uploads 卷。Redis 若要保留队列/会话，需在停止相关写入后备份其持久化文件。
4. 在远程 Ubuntu 使用相同提交、配置和兼容版本的镜像；创建空卷后还原数据，再启动服务。
5. 先保留 `127.0.0.1:8088` 监听，通过 SSH 隧道访问：`ssh -L 8088:127.0.0.1:8088 用户@服务器`。
6. 验证登录、模型调用、数据查询和重启后数据保留；确认完成后再停止本地正式实例。

新机器与本机内存及 CPU 架构可能不同，迁移时应重新核验镜像架构和资源余量。备份内含密钥与个人数据，需加密保存。

## 验证与已知限制

基础部署已验证：前端类型检查和生产打包通过；四个容器健康；Windows 与 WSL 使用 `127.0.0.1:8088` 可访问；管理员登录通过；未认证访问账户接口返回 401。

镜像和代码来源记录在 `deploy/BUILD-MANIFEST.json`，实际 Python 依赖版本记录在 `deploy/requirements.snapshot.txt`。原仓库的 `uv.lock` 对应旧项目，当前构建按 v3.0 的 `pyproject.toml` 安装依赖；快照仅记录版本，尚未替换原仓库锁文件。

前端依赖安装时 npm 报告 44 项依赖安全告警（其中 26 项 high，含开发依赖）；未进行逐项运行路径验证，也没有执行可能破坏兼容性的自动批量升级。迁移公网部署前应另行评估和处理。
