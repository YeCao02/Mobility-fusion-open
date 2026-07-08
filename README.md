# 多源手机 SDK 定位融合与日内 OD 清洗-聚合框架

[English](README_en.md) | [在线 Demo / Live Demo](https://yecao02.github.io/Mobility-fusion-open/demo/) | [上海抽样回放 / Shanghai sample replay](https://yecao02.github.io/Mobility-fusion-open/demo/shanghai-validation/)

本仓库是 **Mobility Fusion** 的公开展示版本，用于说明 PioneerData 多源手机 SDK 原始事件如何被标准化、融合、审计，并构造为可解释的用户日内 `FULL_OD` 链。公开仓库仅包含框架说明、抽样 demo 数据和浏览器，不包含全量原始数据与私有生产配置。

![framework](assets/figures/full_od_strategy_framework_public_zh.png)

## 在线 Demo

[![demo preview](assets/figures/demo_preview.png)](https://yecao02.github.io/Mobility-fusion-open/demo/)

大湾区 demo：[https://yecao02.github.io/Mobility-fusion-open/demo/](https://yecao02.github.io/Mobility-fusion-open/demo/)

上海抽样回放页：[https://yecao02.github.io/Mobility-fusion-open/demo/shanghai-validation/](https://yecao02.github.io/Mobility-fusion-open/demo/shanghai-validation/)

Demo 支持按城市和日期加载样本，并同时显示两类数据：

- `processed`：原始 `mobility-fusion` 生产流程输出的 `FULL_OD` H3 节点和 OD 段。
- `raw`：同一批 uuid-day 对应的标准化原始事件，用于审计哪些点被捕获、吸收、压制或剔除。

大湾区样本为内地九市每城约 1000 个 uuid-day，共 9000 个 uuid-day。上海页面不是单独的质量验证逻辑，而是先从 `S:\GEO BIG data\Shanghai-2026` 的 `residence_*` 压缩原始文件中按天抽样，并用同目录 `poi_info` 将 SceneReco 的 `p_id` 补回 `p_name`，再改写为原始 `mobility-fusion` pipeline 的 `unified_points` 输入格式，最后使用同一套 `FULL_OD` 生产流程计算并转换为本 demo 的 `manifest + processed/raw` 分片格式。

上海样本范围为 2026-05-01 至 2026-05-14。每天按 `WifiStable` 文件内 UUID 首次出现顺序抽取 100 个 uuid-day：第 1 天取第 1-100 个，第 2 天取第 201-300 个，以此类推；同时过滤掉 2026-05-15 00:00:00 及之后的事件。

## 数据口径

四类输入事件：

- `SceneReco`：场景识别 / POI 语义点。
- `WiFiConnect`：Wi-Fi 连接事件。
- `WiFiStable`：稳定 Wi-Fi 锚点事件。
- `Timing`：蜂窝 timing / GPS 类时空点。

核心融合优先级：

```text
SceneReco > WiFiConnect > WiFiStable > Timing
```

公开 demo 使用 `FULL_OD` 口径：完成原始事件标准化、H3 编码、局部支撑校验、近邻吸收、防抖、速度与上下文检查后，保留日内连续的 `DAY_START / STAY / STOP / DAY_END` 节点，并由相邻节点生成 OD 段。

## Demo 状态说明

浏览器中的原始点状态是公开审计标记：

- `K`：标准化事件被生产节点直接捕获或作为可信证据保留。
- `S`：事件没有成为最终代表 H3 节点，通常表示被邻近代表节点吸收、压制或未匹配。
- `D`：原始阶段已有明确异常或硬剔除标记。

这些状态用于解释 demo 可视化，不等同于全量生产环境内部所有私有审计字段。

## 仓库结构

```text
.
|-- assets/figures/
|   |-- full_od_strategy_framework_public_zh.png
|   |-- full_od_strategy_framework_public_en.png
|   `-- demo_preview.png
|-- demo/
|   |-- index.html
|   |-- shanghai-validation/
|   `-- data/
|       |-- manifest.json
|       |-- processed/
|       `-- raw/
`-- scripts/
    |-- build_open_demo_data.py
    |-- prepare_shanghai_unified_sample.py
    `-- build_shanghai_open_demo_data.py
```

## 重新生成公开 Demo 数据

```powershell
& "E:\ANACONDA\envs\GEO\python.exe" ".\scripts\build_open_demo_data.py" `
  --out ".\demo\data" `
  --per-city 1000
```

## 重新生成上海抽样回放数据

第一步，将上海 `residence_*` 抽样改写为原始 pipeline 的 `unified_points` 输入：

```powershell
& "E:\ANACONDA\envs\GEO\python.exe" ".\scripts\prepare_shanghai_unified_sample.py" `
  --out-root "S:\GEO BIG data\Shanghai-2026\sample_unified_points" `
  --threads 24 `
  --batch-size 50000 `
  --batches-per-read 4 `
  --overwrite
```

第二步，在本地 `mobility-fusion` 完整代码仓库中运行原始生产 pipeline，`--spatial-filter-scope none` 用于避免沿用广佛空间过滤：

```powershell
& "E:\ANACONDA\envs\GEO\python.exe" "pipeline\run_production_pipeline.py" `
  --raw-root "S:\GEO BIG data\Shanghai-2026\sample_unified_points" `
  --raw-format unified_points `
  --out-root "S:\GEO BIG data\Shanghai-2026\sample_mobility_fusion_v0_4_0" `
  --date 2026-05-01 --date 2026-05-02 --date 2026-05-03 --date 2026-05-04 `
  --date 2026-05-05 --date 2026-05-06 --date 2026-05-07 --date 2026-05-08 `
  --date 2026-05-09 --date 2026-05-10 --date 2026-05-11 --date 2026-05-12 `
  --date 2026-05-13 --date 2026-05-14 `
  --canonical-h3-res 10 `
  --parent-h3-res 9 `
  --range-shard-target-rows 500000 `
  --od-range-target-rows 200000 `
  --ingest-polars-threads 4 `
  --od-step-workers 12 `
  --od-polars-threads 1 `
  --od-start-stagger-sec 1 `
  --core-output-only `
  --no-analysis-tables `
  --spatial-filter-scope none `
  --force
```

第三步，将原始 pipeline 输出转换为公开 demo 的同一套数据分片：

```powershell
& "E:\ANACONDA\envs\GEO\python.exe" ".\scripts\build_shanghai_open_demo_data.py"
```
