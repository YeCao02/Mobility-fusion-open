# Mobility Fusion 公开演示：冻结 v8 存量轨迹

[English](README_en.md) · [大湾区演示](https://yecao02.github.io/Mobility-fusion-open/demo/) · [上海演示](https://yecao02.github.io/Mobility-fusion-open/demo/shanghai-validation/) · [Pipeline 演进与存储说明](https://yecao02.github.io/Mobility-fusion-open/docs/pipeline-20261028/REPORT.html)

本次演示直接从已完成并验收的全量 frozen-v8 发布包抽取，每个城市固定 **2,000 个唯一数据身份**，共九个大湾区内地城市18,000个、上海residence样本2,000个。已完成轨迹不重新构链。

| 演示 | 城市 | 实际交付日期 | 唯一用户样本 | 数据投影 |
|---|---|---|---:|---|
| 大湾区 | 广州、深圳、珠海、佛山、江门、肇庆、惠州、东莞、中山 | 2024-12-24、2024-12-29 | 每城2,000，总18,000 | 2,250,737 raw行、213,433节点、183,366线段 |
| 上海 | 上海residence cohort | 2026-05-01至2026-05-14 | 固定2,000 | 2,313,740 raw日视图行、201,335节点、177,843线段 |

两地时间不同，不是同期人口对比。每个样本保留交付日期范围内**全部可用用户上下文**；缺观测的样本日期显示空槽位，不补造观测。页面按城市、日期和200个样本ID分页，仅获取当前页的一对processed/raw gzip分片；切换日期可追踪同一本次伪名。新口径是固定跨日用户，取代旧GBA每城约1,000个uuid-day和旧上海每日300个不同uuid-day的展示口径。

## 地图底图

采用 `https://tile.openstreetmap.org/{z}/{x}/{y}.png`，无需API key，地图保留可见的© OpenStreetMap contributors署名。使用浏览器常规缓存，不预下载或批量抓瓦片。服务可用性以OSMF政策为准：[官方瓦片政策](https://operations.osmfoundation.org/policies/tiles/)、[版权与署名](https://www.openstreetmap.org/copyright)。

## 数据解释与隐私范围

- 本次公开ID由独立私钥HMAC派生；源UUID、盐值与私有映射不发布。地点名称不公开。原坐标（按既有演示精度）和时间仍用于方法展示，**伪名不等于不可重识别保证**。
- 节点/线段来自已验收发布包；浏览器JSON为精简可视化投影，不是七表无损存档，不替代完整字段合同。raw是与正式输入一致的标准化分析日视图，不是未处理供应商原文件。
- `K/S/D`为展示标记：K为主H3与时间包络匹配，S为未匹配到当前展示节点，D仅引用输入显式质量标记；它们不是冻结引擎完整私有保留/剔除血缘，也不等于事实真值。
- 样本城市按最早可用日期DAY_START代表H3映射至已有大湾区城市表；不代表居住城市或主要活动城市。节点实际行政城市独立标示，表外为未知；线段两端都已知才给true/false跨城，其余为null/待判。上海样本所属cohort不自动填充每个节点的城市。
- 这是可复现的分区候选方便抽样，不是人口概率样本；不能以样本数作总体出行率分母。连线不是道路导航轨迹，跨城连线也不是独立验证的真实行程。

## 来源与校验

大湾区源build `7f0f600b9ebef7342dbc2fc8ebc3dc84bfd2e1793a369629320a6bf03c334b49`；上海residence源build `795f903d6f71540dcb46d3a404dd78eda44507f72afc541070d7a9221119412f`。导出前用包内reader校验抽中分区并解码，不复跑轨迹生成。公开数据逐身份/日期与私有选定上下文核对raw、nodes、segments行数及段端点；公开JSON扫描不含源UUID格式。

[详细抽样与投影协议](docs/demo-refresh-20261028/METHOD.md) · [公开数据校验回执](docs/demo-refresh-20261028/PUBLIC_DATA_VALIDATION.json)

`demo/data/manifest_v8.json`与`demo/shanghai-validation/data/manifest_v8.json`为当前入口。旧演示数据已从当前树替换，原版本保留Git历史。旧脚本仅供历史方法参考；当前导出脚本为`scripts/build_v8_public_demo.py`，需本地私有已验收输入与身份映射环境，不包含全量数据或生产私有配置。
