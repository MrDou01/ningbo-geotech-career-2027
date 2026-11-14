# 云端每日更新

GitHub Actions每日北京时间08:30运行，排队时可能延迟。电脑关机、Codex关闭均不影响GitHub托管运行器。手动或代码推送验收24页，日常定时每轮最多120页，优先临期/宁波/高匹配并轮换省份与招聘栏目。

两站共用 data/dashboard.json；既有密码门不变。Sites运行时读取同一公开数据源，日常仅更新数据，无需重复发布Sites。

自动处理：读取官方来源、记录HTTP与正文哈希变化、抓取同域新招聘链接到独立待核库、按已记录的官方截止信息标历史、有限下载公务员附件原件并保留历史、输出JSON与SQLite，构建Pages发布包并校验线上时间戳。

范围：这不是人工资格审核。新线索不自动当现招，2027资格、人才档次、论文认定和期刊分区不凭网页200或关键词自动确认。人工核验日期不随抓取刷新。验证码、登录、访问限制不绕过。仅抓现有已记录来源与可发现的同域链接，不承诺全网穷尽。

失败：Actions失败会留下日志与诊断artifact，保留上次成功网站；通知由仓库Actions通知设置控制。详情：https://github.com/MrDou01/ningbo-geotech-career-2027/actions/workflows/daily.yml

数据库下载：https://mrdou01.github.io/ningbo-geotech-career-2027/data/jobs.sqlite
数据下载：https://mrdou01.github.io/ningbo-geotech-career-2027/data/dashboard.json

本地电脑开机后，在04-在线网页执行npm run sync:cloud可下载云端JSON/SQLite，覆盖前保存历史副本；不会改写本地私有成果档案。断网时保留现有缓存。今后构建默认先取云端，避免旧快照倒灌；有新人工证据时需显式运行sync:data并审查合并，不直接覆盖更新的云端数据。

公开仓库只含已授权的求职资料及访问密码摘要；不添加API密钥、Cookie或明文密码。GitHub工作流使用平台短期GITHUB_TOKEN，未新增外部计费服务。
