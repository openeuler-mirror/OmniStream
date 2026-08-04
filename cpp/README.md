# OmniStream Native 构建指南

本文档介绍 OmniStream Native 项目中 Agent 模块和 Omni 模块的构建方法。

## 1. 进入 C++ 项目目录

执行以下命令进入 C++ 项目目录：

```shell
cd cpp
```

## 2. 构建 Agent 模块

### PCRE2 依赖

Agent 模块依赖 PCRE2。若当前环境尚未安装 PCRE2 开发包，可执行以下命令进行安装：

```shell
yum install -y pcre2-devel
```

关闭 TNEL 模块，仅构建 Agent 模块：

```shell
cmake -S . -B build-agent \
  -DBUILD_TNEL=OFF \
  -DBUILD_AGENT=ON

cd build-agent
make
```

构建成功后，生成的动态库位于：

```text
build-agent/agent/libregex.so
```

## 3. 构建 Omni 模块

开启 TNEL 模块，并关闭 Agent 模块：

```shell
cmake -S . -B build-omni \
  -DBUILD_TNEL=ON \
  -DBUILD_AGENT=OFF

cd build-omni
make
```
