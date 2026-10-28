# 本机公告证据研究

独立于行情服务和朋友账户的可选模块，默认 `http://127.0.0.1:8010/`。上交所公开 HTML → 候选字段 → 原文核验 → SQLite事实与查询看板；用实际 LangGraph SQLite 检查点恢复。首版自动核对上市公告，其他类型会停在人工核验，不能据此声称覆盖所有公告。

## 安装和启动

Windows CPython 3.14.2 已实测。建议在源码目录建立独立环境；本机存在全局 PYTHONPATH，以下 `-I` 防止串入行情环境：

```powershell
python -m venv .venv-announcements
.\.venv-announcements\Scripts\python.exe -I -m pip --isolated install --index-url https://pypi.org/simple -r requirements-announcements.txt
.\.venv-announcements\Scripts\python.exe -I -m pip --isolated check
.\.venv-announcements\Scripts\python.exe -I announcement_lab/serve.py
```

也可把已装好此依赖的 Python 绝对路径设置为 `ANNOUNCEMENT_PYTHON`，运行根目录 `启动公告研究.bat`。启动器不安装依赖、不读取行情配置、不启动分享。数据默认独立存放 `announcement_data/`，可用 `--data-root` 或 `ANNOUNCEMENT_DATA_ROOT` 指定新目录。旧原型模式不兼容时会停止、保留原文件，不自动覆盖或迁移。现阶段无 Docker 构建/运行验收，勿按容器已部署理解。

## 先运行无费用示例

网页“运行公开示例”使用留存的[上交所原文](https://www.sse.com.cn/disclosure/announcement/listing/c/c_20201027_5242851.shtml)，是2020年历史资料。规则基线不调用模型；发布日期2020-10-28与上市日2020-10-29分别记录，发行70409170份、首批上市57664136份，原数值/单位和引文一起保留。人工整理参考不是人工金标准，单条成功不证明模型准确率。

当前只允许 `www.sse.com.cn` 公告 HTTPS HTML 路径，每次跳转也检查来源；PDF、任意URL及登录页面不支持。粘贴文本标记来源未远端核实。内容和元数据哈希只能检验保存内容一致，不能证明发布者签名或公告事实真实。

点“定位原文”核对字段，填写复核说明后确认或不采纳。待核验候选可追加修订；已入库记录的修订会创建关联任务/新事实，保留旧事实及原始模型候选，不自动代表官方更正公告。重复相同修订不重复创建任务。两份正文同 URL 的先后版本只记录版本关系，不擅自认定为正式更正。

## 模型和权限

默认本机基线不设令牌，其他本机程序也可能访问这个公开研究库；可配置独立 `ANNOUNCEMENT_ACCESS_TOKEN`。网页仅在本标签页会话保存该机主令牌。真实模型还须在服务器配置独立 `ANNOUNCEMENT_MODEL_KEY`，且每次提交勾选发送公开正文/费用同意；不读取行情项目 Key。不要把任何 Key、交易凭据放到网页、包、镜像或报告中。

适配器按2026-10-26[官方模型/API文档](https://api-docs.deepseek.com/api/create-chat-completion)采用 `deepseek-flash` 非思考模式和[JSON输出](https://api-docs.deepseek.com/guides/json_mode/)，本轮只完成模拟 HTTP 协议验证，**真实模型尚未调用/评测**。模型名是会变化的服务别名，不是不可变权重版本；保存返回模型/响应ID及实际 usage。没有真实用量不预报金额，费用以供应商当前账单为准；JSON为空/无效也保存原始响应供修订，不自动付费重试。

外部调用已发出但结果未知时，原任务和同正文新任务均停止自动调用；核对供应商后，由机主明确接受重复计费风险才恢复。已保存响应或已完成业务入库可复用，不靠检查点保证收费。断开浏览器不代表任务停止，重新读取任务即可查看持久状态。

非本机绑定需显式 `ANNOUNCEMENT_BIND_HOST`、至少24字符独立机主令牌及具体 `ANNOUNCEMENT_ALLOWED_HOSTS`，目前尚无公网部署验收，不接入 cpolar、朋友账户或交易操作。跨站写、任意 Host、过大正文会被拒绝。

## 备份与恢复边界

`Repository.backup(新文件路径)` 用 SQLite 在线备份产生一致业务库，保存返回 SHA；`Repository.restore_new(备份,不存在的新目录,预先核实的SHA)` 禁止覆盖原目录。备份包括原文、任务、外部调用账和事实，不含 LangGraph 检查点、模型Key或行情私人记录。恢复后先核对原任务/外部调用状态；不因丢检查点重新付费。当前已用合成数据验证，尚未执行用户真实数据迁移。损坏/未知模式停止，不初始化覆盖。
