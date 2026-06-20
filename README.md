# 多源手机 SDK 定位数据融合与日内 OD 清洗-聚合框架

[English](README_en.md) | [在线 Demo / Live Demo](https://yecao02.github.io/Mobility-fusion-open/demo/)

本仓库是 **Mobility Fusion** 的公开展示版本，用于说明 PioneerData 多源手机 SDK 原始事件如何被标准化、融合、审计，并构造为可解释的用户日内 `FULL_OD` 链。公开仓库仅包含框架说明、抽样 demo 数据和浏览器，不包含全量原始数据与私有生产配置。

![framework](assets/figures/full_od_strategy_framework_public_zh.png)

## 在线 Demo

[![demo preview](assets/figures/demo_preview.png)](https://yecao02.github.io/Mobility-fusion-open/demo/)

Demo 地址：[https://yecao02.github.io/Mobility-fusion-open/demo/](https://yecao02.github.io/Mobility-fusion-open/demo/)

Demo 支持按城市加载样本，并同时显示两类数据：

- `processed`：生产管线输出的 `FULL_OD` H3 节点和 OD 段。
- `raw`：同一批 uuid-day 对应的标准化原始事件，用于审计哪些点被捕获、吸收、压制或剔除。

样本规模为大湾区内地九市每城随机抽取约 1000 个 uuid-day，共 9000 个 uuid-day。为避免单文件过大，数据按城市和日期分片保存。

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

当前公开 demo 使用 `FULL_OD` 口径：完成原始事件标准化、H3 编码、局部支撑校验、近邻吸收、防抖、速度与上下文检查后，保留日内连续的 `DAY_START / STAY / STOP / DAY_END` 节点，并由相邻节点生成 OD 段。

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
|   `-- data/
|       |-- manifest.json
|       |-- processed/
|       `-- raw/
`-- scripts/
    `-- build_open_demo_data.py
```

## 重新生成公开 Demo 数据

脚本默认从本地生产目录读取：

```text
S:\GEO BIG data\Greater Bay Area data_operators 500G\mobility_fusion_production_v0_4_0
```

运行方式：

```powershell
& "E:\ANACONDA\envs\GEO\python.exe" "./scripts/build_open_demo_data.py" `
  --out "./demo/data" `
  --per-city 1000
```

脚本使用 Polars 按城市和日期分块读取 Parquet，避免一次性加载全量事件到内存。
