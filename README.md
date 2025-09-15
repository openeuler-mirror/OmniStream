# OmniStream

# 项目介绍

## 背景

大数据实时处理流计算引擎在互联网、金融、物流等各行各业应用广泛，承担不可或缺的重要角色，而随着业务发展，流计算引擎的性能逐渐成为瓶颈。以主流实时流计算引擎Flink为例，主要采用Java/Scala等高级编程语言实现，当前对Flink的优化主要是基于Java进行改进，但Java的本身的性能还是弱于Native Code，并且由于Java的语义的限制，对于整体的类SIMD指令支持较弱，无法完全发挥CPU的算力。此外，Flink采用行式数据结构计算流数据，无法充分利用芯片的向量化指令，执行效率有待提升。

## OmniStream介绍

OmniStream算子加速采用Native Code（C/C++）实现Flink SQL和DataStream算子。针对Flink SQL，OmniStream采用C++结合向量化指令实现算子，以列式内存数据格式OmniVec进行内存计算，充分利用向量化加速，提升SQL计算性能。针对Flink DataStream，OmniStream采用C++结合向量化指令实现算子，结合UDF翻译工具将UDF翻译成C++程序，充分发挥Native Code性能优势，提升DataStream场景性能。

# 版本说明

**当前版本适用于开源软件哪个版本，如**

| 开源软件 | 开源版本 |
| -------- | -------- |
| Flink    | 1.16.3   |


# 快速上手

## 源码编译

### 1、依赖组件

- GCC: 10.3.1
- CMAKE: 3.22.0
- JDK: 1.8.0_342
- zlib: 1.2.8
- LLVM: 12.0.1
- googletest: 1.10.0
- jemalloc: 5.2.1
- nlomann json: 3.11.3
- huawei securec: V100R001C01SPC011B003_00001
- OmniOperator: 20250630
- snappy: 1.1.10
- rocksdb: 8.11.4
- BoostKit-kaccjson: 1.0.0
- BoostKit-ksl: 2.5.1

### 2、编译命令

编译OmniStream前需要编译OmniAdaptor代码，编译命令如下

```
cd omniop-flink-extension
mvn clean package -DskipTests
```

OmniStream编译命令如下

```
cd OmniSteam/cpp
mkdir build
cd build
cmake ..
make install -j$PARALLELISM 
```

PARALLELISM: 编译指定并行度

## 环境部署

部署请参考以下链接：

https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_09018.html

## 测试验证

**快速验证安装成功的方法或者快速执行方法，如**

进入Flink安装目录下的bin目录，并启动Flink

```
cd $FLINK_HOME/bin/ && ./start-cluster.sh
```

调用sql-client后，进行测试

```
./sql-client.sh
```

在命令行中输入

```
SELECT 'Hello, Flink!';
```

可以正常输出结果即安装正常。

# 贡献指南

如果使用过程中有任何问题，或者需要反馈特性需求和bug报告，可以提交isssues联系我们，具体贡献方法可参考[这里](https://gitcode.com/boostkit/community/blob/master/docs/contributor/contributing.md)。

# 免责声明

此代码仓计划参与Flink软件开源，仅作Flink功能扩展/Flink性能提升，编码风格遵照原生开源软件，继承原生开源软件安全设计，不破坏原生开源软件设计及编码风格和方式，软件的任何漏洞与安全问题，均由相应的上游社区根据其漏洞和安全响应机制解决。请密切关注上游社区发布的通知和版本更新。鲲鹏计算社区对软件的漏洞及安全问题不承担任何责任。

# 许可证书

**若是参与开源，参考上游社区所用开源协议，在代码仓根目录下放置LICENSE文件。
若是主导开源，则自行决定开源协议类型，然后同样代码仓根目录下放置LICENSE文件**

# 参考文档

安装指南：

https://www.hikunpeng.com/document/detail/zh/kunpengbds/appAccelFeatures/sqlqueryaccelf/kunpengbds_omniruntime_20_09018.html

