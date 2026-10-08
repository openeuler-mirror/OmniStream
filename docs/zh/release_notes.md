# 版本说明书

## 版本配套说明

### 产品版本信息

| 项目          | 内容                  |
|-------------|---------------------|
| 产品名称        | Kunpeng BoostKit    |
| 产品版本        | 26.2.RC1            |
| 软件名称        | OmniStream          |
| 软件版本        | 1.4.0               |

### 软件版本配套说明

|项目| 版本                                                                                                                                                           |
|--|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
|操作系统| [openEuler 22.03 LTS SP4](https://dl-cdn.openeuler.openatom.cn/openEuler-22.03-LTS-SP4/ISO/aarch64/openEuler-22.03-LTS-SP4-everything-debug-aarch64-dvd.iso) |
|JDK| [毕昇JDK 17（建议使用毕昇JDK 17.0.18-b13）](https://mirrors.huaweicloud.com/kunpeng/archive/compiler/bisheng_jdk/bisheng-jdk-17.0.18-b13-linux-aarch64.tar.gz)         |
|Flink| [1.16.3](https://archive.apache.org/dist/flink/flink-1.16.3/flink-1.16.3-bin-scala_2.12.tgz)                                                                 |
|Docker| [19.03.15](https://www.hikunpeng.com/document/detail/zh/kunpengboostkithistory/251RC1/bds/kunpengbds_omniruntime_20_0911.html)                               |
|Nexmark| v0.3.0                                                                                                                                                       |

### 硬件版本配套说明

| 服务器类型 | 处理器型号     | 内存大小   |
|-------|-----------|--------|
| 鲲鹏服务器 | 920新型号处理器 | 32GB以上 |

### 病毒扫描结果

本软件包、版本文档、产品文档经过防病毒软件扫描，未发现病毒。详细信息如下：

| 项目                | 内容                 |
|-------------------|--------------------|
| Engine Name       | clamav             |
| Engine Version    | 1.0.9              |
| Virus Lib Version | 28135              |
| Scan Time         | 2026-09-30 15:42:47 |
| Scan Result       | OK                 |

## V1.4.0

### 更新说明

V1.4.0补充了checkpoint/savepoint的支持。

**新增特性**

- 支持checkpoint/savepoint。
- SQL场景下，支持Join/Deduplicate/Rank算子通过canonical格式的savepoint与Flink进行作业切换。

**修改特性**

无

**删除特性**

无

### 已解决的问题

无

### 遗留问题

无

## V1.3.0

### 更新说明

V1.3.0扩展了SQL场景下的算子、内置函数和数据类型支持，并支持Calc算子注册UDF函数。

**新增特性**

- Calc算子支持UDF函数注册，Calc算子支持JSON_VALUE、JSON_QUERY、COALESCE、PROCTIME_MATERIALIZE、CHAR_LENGTH、TO_TIMESTAMP_LTZ内置函数。
- Calc算子支持INTEGER、TIMESTAMP_WITH_LOCAL_TIMEZONE(3)数据类型。
- 在SQL场景中，支持WindowAgg、WindowJoin算子。

**修改特性**

无

**删除特性**

无

### 已解决的问题

无

### 遗留问题

无

## V1.2.0

### 更新说明

V1.2.0补充UDF翻译工具依赖的头文件，并新增OmniStateStore加速特性，提升有状态场景的执行性能。

**新增特性**

- 增加UDF翻译工具所使用依赖的头文件安装内容。
- 针对有状态场景，增加OmniStateStore加速特性，通过降低RocksDB访问频次提升应用性能。

**修改特性**

无

**删除特性**

无

### 已解决的问题

无

### 遗留问题

无

## V1.1.0

### 更新说明

V1.1.0增强SQL算子回退能力，并完善DataStream KeyedCoProcess算子的checkpoint和restore支持。

**新增特性**

- SQL：新增支持task级别算子回退机制。
- DataStream：KeyedCoProcess算子支持checkpoint、restore。

**修改特性**

无

**删除特性**

无

### 已解决的问题

无

### 遗留问题

无

## V1.0.0

### 更新说明

V1.0.0首次提供SQL和DataStream常用算子的Native化加速能力，并支持基础UDF翻译及状态后端。

**新增特性**

SQL

- 实现了Calc、GroupAgg、Join、Deduplicate、Rank、Window、Kafka Source/Sink算子加速。
- 实现了高效数据组织方式OmniVec。
- 实现了对内存和RocksDB状态后端的支持。

DataStream

- 实现了Kafka Source、Kafka Sink、Map、FlatMap、 Reduce、Filter算子加速。
- 实现了UDF基础框架及翻译基础库，支持UDF自动Native化框架，成功通过DataStream Wordcount等有状态和无状态用例。
- 实现了对内存状态后端的支持。

**修改特性**

无

**删除特性**

无

### 已解决的问题

无

### 遗留问题

无

## 版本配套文档

| 文档名称               | 内容简介                                | 交付形式 |
|--------------------|-------------------------------------|------|
| 《OmniStream 版本说明书》 | 提供OmniStream的版本更新内容与发布说明            | 开源仓  |
| 《OmniStream 快速入门》  | 提供OmniStream的快速上手教程，帮助用户快速了解和使用该组件。 | 开源仓  |
| 《OmniStream 编译指南》  | 提供OmniStream的编译指导。                  | 开源仓  |
| 《OmniStream 安装指南》  | 提供OmniStream的安装部署指导。                | 开源仓  |
| 《OmniStream 用户指南》  | 提供OmniStream的使用指导。                  | 开源仓  |
| 《OmniStream 常见问题》  | 记录安装、部署和使用过程中可能遇到的问题及其解决方法。         | 开源仓  |

### 获取文档的方法

您可以通过访问[开源仓](https://gitcode.com/openeuler/OmniStream)浏览和获取相关文档。

| 文档版本 | 发布日期 | 修改说明 |
| --- | --- | --- |
| 01 | 2026-09-30 | 第一次正式发布。 |
