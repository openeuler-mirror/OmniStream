# 快速入门

本文以ARM架构的openEuler 22.03 LTS SP4操作系统为例，介绍OmniStream的安装和基本使用方法。

## 组件介绍

OmniStream Flink Native化是OmniRuntime的特性之一。OmniStream通过Native Code（C/C++）实现Flink SQL与DataStream算子，在保持Flink原有开发习惯和架构兼容性的基础上，提高查询执行效率。

当前OmniStream支持Flink 1.16.3版本。有关OmniStream架构、支持的算子及使用限制，请参见[项目介绍](../../README.md)。

## 快速安装

### 1. 安装Docker

```bash
yum install -y docker
systemctl start docker
```

执行以下命令验证Docker是否安装成功：

```bash
docker version
```

> 下图是已安装信息。
> ![Docker版本信息](figures/quick-start/docker-version.png)

### 2. 导入openEuler镜像

```bash
wget --no-check-certificate https://mirrors.huaweicloud.com/openeuler/openEuler-22.03-LTS-SP4/docker_img/aarch64/openEuler-docker.aarch64.tar.xz
docker load -i openEuler-docker.aarch64.tar.xz
```

执行以下命令验证镜像是否导入成功：

```bash
docker images
```


### 3. 创建并进入容器

执行以下命令检查宿主机的30211端口是否被占用：

```bash
ss -tuln | grep -w 30211
```

命令没有输出表示端口未被占用。如果端口已被占用，请将后续命令中的30211替换为其他空闲端口。

执行以下命令创建容器。`YourContainName`为示例容器名，根据实际情况进行替换。容器的8081端口映射到宿主机的30211端口，用于访问Flink Web UI。

```bash
CONTAINER_NAME=YourContainName
docker run -itd --name $CONTAINER_NAME --hostname $CONTAINER_NAME --privileged=true -p 0.0.0.0:30211:8081 openeuler-22.03-lts-sp4 /bin/bash
```

进入容器：

```bash
docker exec -it YourContainName /bin/bash --login
```

以下操作均在容器内以root用户执行。

### 4. 安装基础依赖

```bash
yum install -y wget findutils unzip libXext libX11 libXrender libXtst libXi
```

> 如果所在环境需要通过代理访问网络，需要根据实际网络环境配置代理。

### 5. 安装JDK

```bash
mkdir -p /usr/local
cd /usr/local
JDK_TAR="bisheng-jdk-17.0.18-b13-linux-aarch64.tar.gz"
wget --no-check-certificate "https://mirrors.huaweicloud.com/kunpeng/archive/compiler/bisheng_jdk/${JDK_TAR}"
JDK_DIR=$(tar -tf "${JDK_TAR}" | head -1 | cut -d/ -f1)
tar -zxf "${JDK_TAR}"
chown -R root:root "/usr/local/${JDK_DIR}"
ln -sfn "/usr/local/${JDK_DIR}" /usr/local/java
rm -f "${JDK_TAR}"
```

配置JDK环境变量：

```bash
echo 'export JAVA_HOME=/usr/local/java' >> /etc/profile
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> /etc/profile
echo 'export C_INCLUDE_PATH=$JAVA_HOME/include:$JAVA_HOME/include/linux:$C_INCLUDE_PATH' >> /etc/profile
echo 'export CPLUS_INCLUDE_PATH=$JAVA_HOME/include:$JAVA_HOME/include/linux:$CPLUS_INCLUDE_PATH' >> /etc/profile
echo 'export LIBRARY_PATH=$JAVA_HOME/lib:$JAVA_HOME/lib/server:$LIBRARY_PATH' >> /etc/profile
echo 'export LD_LIBRARY_PATH=$JAVA_HOME/lib:$JAVA_HOME/lib/server:$LD_LIBRARY_PATH' >> /etc/profile
source /etc/profile
```

执行以下命令验证JDK是否安装成功：

```bash
java -version
```

> 执行结果
> ![JDK版本信息](figures/quick-start/jdk-version.png)

### 6. 安装Flink

```bash
mkdir -p /usr/local
cd /usr/local
FLINK_TAR="flink-1.16.3-bin-scala_2.12.tgz"
wget --no-check-certificate "https://mirrors.huaweicloud.com/apache/flink/flink-1.16.3/${FLINK_TAR}"
FLINK_DIR=$(tar -tf "${FLINK_TAR}" | head -1 | cut -d/ -f1)
tar -zxf "${FLINK_TAR}"
chown -R root:root "/usr/local/${FLINK_DIR}"
ln -sfn "/usr/local/${FLINK_DIR}" /usr/local/flink
rm -f "${FLINK_TAR}"
echo 'export FLINK_HOME=/usr/local/flink' >> /etc/profile
source /etc/profile
```

下载JSON和Gson依赖：

```bash
cd "$FLINK_HOME/lib"
wget --no-check-certificate https://repo.maven.apache.org/maven2/org/json/json/20240303/json-20240303.jar
wget --no-check-certificate https://repo.maven.apache.org/maven2/com/google/code/gson/gson/2.11.0/gson-2.11.0.jar
```

执行以下命令检查依赖是否下载成功：

```bash
ls -la "$FLINK_HOME/lib" | grep -E "json|gson"
```

> 检查结果
> ![Flink的JSON和Gson依赖](figures/quick-start/flink-json-gson-libs.png)

执行以下命令验证Flink是否安装成功：

```bash
"$FLINK_HOME/bin/flink" --version
```

> 检查结果
> ![Flink版本信息](figures/quick-start/flink-version.png)

### 7. 安装OmniStream和依赖库

从[OmniStream发布页面](https://gitcode.com/openeuler/OmniStream/releases)下载当前版本的以下软件包：

- `BoostKit-omniruntime-omnistream-{version}.zip`
- `Dependency_library_OmniStream.zip`

以下命令以OmniStream 1.3.0版本为例：

```bash
mkdir -p /opt/omnistream-packages
cd /opt/omnistream-packages
wget --no-check-certificate https://gitcode.com/openeuler/OmniStream/releases/download/tag_BoostKit_26.1.RC1.B030_001/BoostKit-omniruntime-omnistream-1.3.0.zip
wget --no-check-certificate https://gitcode.com/openeuler/OmniStream/releases/download/tag_BoostKit_26.1.RC1.B030_001/Dependency_library_OmniStream.zip
unzip BoostKit-omniruntime-omnistream-1.3.0.zip
unzip Dependency_library_OmniStream.zip
```

安装依赖库：

```bash
DEPENDENCY_DIR=$(find /opt/omnistream-packages -type d -name Dependency_library_Default -print -quit)
test -n "${DEPENDENCY_DIR}"
mkdir -p /opt/Dependency_library
cp -rf "${DEPENDENCY_DIR}/"* /opt/Dependency_library/
chmod -R 550 /opt/Dependency_library/*
```

执行以下命令检查依赖库：

```bash
ls -la /opt/Dependency_library
```

> 应看到
> ![OmniStream依赖库](figures/quick-start/dependency-libraries.png)

安装OmniStream：

```bash
OMNISTREAM_DIR=$(find /opt/omnistream-packages -type d -name OmniStream_Default -print -quit)
test -n "${OMNISTREAM_DIR}"
mkdir -p /usr/local/OmniStream
cp -rf "${OMNISTREAM_DIR}/"* /usr/local/OmniStream/
chmod -R 550 /usr/local/OmniStream/*
```

执行以下命令检查OmniStream文件：

```bash
ls -la /usr/local/OmniStream
```

> 应看到
> ![OmniStream安装文件](figures/quick-start/omnistream-files.png)

配置动态库搜索路径：

```bash
echo 'export LD_LIBRARY_PATH=/opt/Dependency_library:/usr/local/OmniStream:$LD_LIBRARY_PATH' >> /etc/profile
source /etc/profile
```

执行以下命令检查`libtnel.so`的依赖是否完整：

```bash
ldd /usr/local/OmniStream/libtnel.so | grep "not found"
```

命令没有输出表示依赖完整。

### 8. 配置Flink

编辑Flink配置脚本：

```bash
vi "$FLINK_HOME/bin/config.sh"
```

找到`constructFlinkClassPath`函数，在函数末尾注释原有的`echo`命令，并添加以下内容：

```bash
# echo "$FLINK_CLASSPATH""$FLINK_DIST"
PATCH=/usr/local/OmniStream/flink-tnel-0.1-SNAPSHOT.jar
echo $PATCH:"$FLINK_CLASSPATH""$FLINK_DIST"
```

按`Esc`键，输入`:wq`，按`Enter`键保存并退出。

> 修改结果
> ![Flink类路径配置](figures/quick-start/flink-classpath-config.png)

编辑Flink配置文件：

```bash
vi "$FLINK_HOME/conf/flink-conf.yaml"
```

在文件末尾添加以下配置。该配置必须位于同一行中，不能将参数拆分为多行：

```yaml
env.java.opts: -Djava.library.path=/usr/local/OmniStream:/opt/Dependency_library --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/sun.security.ssl=ALL-UNNAMED --add-exports java.base/sun.net.dns=ALL-UNNAMED --add-exports java.base/sun.net.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.math=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED
```

按`Esc`键，输入`:wq`，按`Enter`键保存并退出。

> 配置结果
> ![Flink JVM参数配置](figures/quick-start/flink-jvm-options.png)

### 9. 安装Nexmark

```bash
cd /usr/local
wget --no-check-certificate https://github.com/nexmark/nexmark/releases/download/v0.2.0/nexmark-flink.tgz
tar -zxf nexmark-flink.tgz
mv nexmark-flink nexmark
chown -R root:root /usr/local/nexmark
rm -f nexmark-flink.tgz
cp /usr/local/nexmark/lib/nexmark-flink-0.2-SNAPSHOT.jar "$FLINK_HOME/lib/"
```

编辑Nexmark配置脚本：

```bash
vi /usr/local/nexmark/bin/config.sh
```

在文件末尾添加以下配置。该配置必须位于同一行中：

```bash
export JAVA_TOOL_OPTIONS="-Djava.library.path=/usr/local/OmniStream:/opt/Dependency_library --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.security.ssl=ALL-UNNAMED --add-exports=java.base/sun.net.dns=ALL-UNNAMED --add-exports=java.base/sun.net.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED"
```

按`Esc`键，输入`:wq`，按`Enter`键保存并退出。

> 配置结果
> ![Nexmark JVM参数配置](figures/quick-start/nexmark-jvm-options.png)

## 快速开始

### 1. 启动Flink

```bash
source /etc/profile
"$FLINK_HOME/bin/start-cluster.sh"
```

执行`jps`检查Flink进程。输出中包含`StandaloneSessionClusterEntrypoint`和`TaskManagerRunner`，表示Flink启动成功。

> Flink进程检查结果
> ![Flink进程](figures/quick-start/flink-processes.png)

### 2. 初始化Nexmark

```bash
bash /usr/local/nexmark/bin/setup_cluster.sh
```

再次执行`jps`。输出中包含`CpuMetricSender`，表示Nexmark初始化成功。

> 检查结果
> ![Nexmark指标采集进程](figures/quick-start/nexmark-process.png)

### 3. 执行Q0用例

```bash
bash /usr/local/nexmark/bin/run_query.sh q0
```

> 任务执行结果
> ![Nexmark Q0执行结果](figures/quick-start/nexmark-q0-result.png)

### 4. 验证OmniStream是否生效

```bash
grep "welcome to native" "$FLINK_HOME"/log/*
```

日志中出现`OmniTask::DoRunInvoke welcome to native`，表示OmniStream已经成功使能。

> 执行结果
> ![OmniStream Native日志](figures/quick-start/omnistream-native-log.png)


## 常见问题

### Flink启动后没有对应的Java进程

执行`ldd /usr/local/OmniStream/libtnel.so | grep "not found"`检查动态库依赖。如果缺少`libXext.so.6`、`libX11.so.6`、`libXrender.so.1`、`libXtst.so.6`或`libXi.so.6`，执行以下命令安装：

```bash
yum install -y libXext libX11 libXrender libXtst libXi
ldconfig
```

### Flink日志提示无法解析配置

`flink-conf.yaml`采用`key: value`格式。`env.java.opts`及其全部参数必须位于同一个物理行中，不能将`--add-opens`或`--add-exports`参数拆分为多行。

### 执行Q0时提示`{"jobs":[]}`

执行`grep -nE "ERROR|Exception|Caused by" /usr/local/nexmark/log/nexmark-flink.log`查看日志。如果日志包含`InaccessibleObjectException`，检查`/usr/local/nexmark/bin/config.sh`中是否已经配置`JAVA_TOOL_OPTIONS`。

## 更多功能

- [编译指南](./compile_guide.md)
- [安装指南](./installation_guide.md)
- [用户指南](./user_guide.md)
- [常见问题](./faq.md)

# 免责声明

此代码仓计划参与Flink软件开源，仅作Flink功能扩展和性能提升。上游软件漏洞与安全问题由相应社区按其安全响应机制处理。
