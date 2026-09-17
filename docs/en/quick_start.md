# Quick Start

This document uses the ARM version of openEuler 22.03 LTS SP4 as an example to describe how to install and verify OmniStream.

## Introduction

OmniStream Flink Native is an OmniRuntime feature. It implements Flink SQL and DataStream operators in native C/C++ code to improve execution efficiency while preserving the existing Flink development model and architecture.

OmniStream currently supports Flink 1.16.3. For its architecture, supported operators, and limitations, see the [project introduction](../../README_en.md).

## Quick Installation

### 1. Install Docker

```bash
yum install -y docker
systemctl start docker
```

Verify the installation:

```bash
docker version
```

Installation information:

![Docker version](../zh/figures/quick-start/docker-version.png)

### 2. Import the openEuler image

```bash
wget --no-check-certificate https://mirrors.huaweicloud.com/openeuler/openEuler-22.03-LTS-SP4/docker_img/aarch64/openEuler-docker.aarch64.tar.xz
docker load -i openEuler-docker.aarch64.tar.xz
```

Verify that the image was imported:

```bash
docker images
```

### 3. Create and enter the container

Check whether port 30211 is already in use on the host:

```bash
ss -tuln | grep -w 30211
```

No output means the port is available. If it is occupied, replace 30211 in the following command with another available port.

`YourContainName` is an example container name. Replace it as needed. Container port 8081 is mapped to host port 30211 for access to the Flink Web UI.

```bash
CONTAINER_NAME=YourContainName
docker run -itd --name $CONTAINER_NAME --hostname $CONTAINER_NAME --privileged=true -p 0.0.0.0:30211:8081 openeuler-22.03-lts-sp4 /bin/bash
```

Enter the container with a login shell so that variables in `/etc/profile` are loaded automatically on subsequent logins:

```bash
docker exec -it YourContainName /bin/bash --login
```

Run all remaining commands as `root` inside the container.

### 4. Install basic dependencies

```bash
yum install -y wget findutils unzip libXext libX11 libXrender libXtst libXi
```

> If your network requires a proxy, configure it according to your environment.

### 5. Install the JDK

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

Configure the JDK environment variables:

```bash
echo 'export JAVA_HOME=/usr/local/java' >> /etc/profile
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> /etc/profile
echo 'export C_INCLUDE_PATH=$JAVA_HOME/include:$JAVA_HOME/include/linux:$C_INCLUDE_PATH' >> /etc/profile
echo 'export CPLUS_INCLUDE_PATH=$JAVA_HOME/include:$JAVA_HOME/include/linux:$CPLUS_INCLUDE_PATH' >> /etc/profile
echo 'export LIBRARY_PATH=$JAVA_HOME/lib:$JAVA_HOME/lib/server:$LIBRARY_PATH' >> /etc/profile
echo 'export LD_LIBRARY_PATH=$JAVA_HOME/lib:$JAVA_HOME/lib/server:$LD_LIBRARY_PATH' >> /etc/profile
source /etc/profile
```

Verify the JDK installation:

```bash
java -version
```

Command output:

![JDK version](../zh/figures/quick-start/jdk-version.png)

### 6. Install Flink

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

Download the JSON and Gson dependencies:

```bash
cd "$FLINK_HOME/lib"
wget --no-check-certificate https://repo.maven.apache.org/maven2/org/json/json/20240303/json-20240303.jar
wget --no-check-certificate https://repo.maven.apache.org/maven2/com/google/code/gson/gson/2.11.0/gson-2.11.0.jar
```

Check the downloaded dependencies:

```bash
ls -la "$FLINK_HOME/lib" | grep -E "json|gson"
```

Dependency files:

![Flink JSON and Gson dependencies](../zh/figures/quick-start/flink-json-gson-libs.png)

Verify the Flink installation:

```bash
"$FLINK_HOME/bin/flink" --version
```

Command output:

![Flink version](../zh/figures/quick-start/flink-version.png)

### 7. Install OmniStream and its dependencies

Download the following packages for the current version from the [OmniStream releases page](https://gitcode.com/openeuler/OmniStream/releases):

- `BoostKit-omniruntime-omnistream-{version}.zip`
- `Dependency_library_OmniStream.zip`

The following commands use OmniStream 1.3.0 as an example:

```bash
mkdir -p /opt/omnistream-packages
cd /opt/omnistream-packages
wget --no-check-certificate https://gitcode.com/openeuler/OmniStream/releases/download/tag_BoostKit_26.1.RC1.B030_001/BoostKit-omniruntime-omnistream-1.3.0.zip
wget --no-check-certificate https://gitcode.com/openeuler/OmniStream/releases/download/tag_BoostKit_26.1.RC1.B030_001/Dependency_library_OmniStream.zip
unzip BoostKit-omniruntime-omnistream-1.3.0.zip
unzip Dependency_library_OmniStream.zip
```

Install the dependency libraries:

```bash
DEPENDENCY_DIR=$(find /opt/omnistream-packages -type d -name Dependency_library_Default -print -quit)
test -n "${DEPENDENCY_DIR}"
mkdir -p /opt/Dependency_library
cp -rf "${DEPENDENCY_DIR}/"* /opt/Dependency_library/
chmod -R 550 /opt/Dependency_library/*
```

Check the dependency directory:

```bash
ls -la /opt/Dependency_library
```

Installed dependencies:

![OmniStream dependency libraries](../zh/figures/quick-start/dependency-libraries.png)

Install OmniStream:

```bash
OMNISTREAM_DIR=$(find /opt/omnistream-packages -type d -name OmniStream_Default -print -quit)
test -n "${OMNISTREAM_DIR}"
mkdir -p /usr/local/OmniStream
cp -rf "${OMNISTREAM_DIR}/"* /usr/local/OmniStream/
chmod -R 550 /usr/local/OmniStream/*
```

Check the OmniStream files:

```bash
ls -la /usr/local/OmniStream
```

Installed files:

![OmniStream files](../zh/figures/quick-start/omnistream-files.png)

Configure the native library search path:

```bash
echo 'export LD_LIBRARY_PATH=/opt/Dependency_library:/usr/local/OmniStream:$LD_LIBRARY_PATH' >> /etc/profile
source /etc/profile
```

Check the dependencies of `libtnel.so`:

```bash
ldd /usr/local/OmniStream/libtnel.so | grep "not found"
```

No output means that all dependencies were found.

### 8. Configure Flink

Open the Flink configuration script:

```bash
vi "$FLINK_HOME/bin/config.sh"
```

Locate `constructFlinkClassPath`, comment out its original `echo` command, and add the following lines at the end of the function:

```bash
# echo "$FLINK_CLASSPATH""$FLINK_DIST"
PATCH=/usr/local/OmniStream/flink-tnel-0.1-SNAPSHOT.jar
echo $PATCH:"$FLINK_CLASSPATH""$FLINK_DIST"
```

Press `Esc`, enter `:wq`, and press `Enter`.

Updated class path:

![Flink class path configuration](../zh/figures/quick-start/flink-classpath-config.png)

Open the Flink configuration file:

```bash
vi "$FLINK_HOME/conf/flink-conf.yaml"
```

Add the following configuration at the end of the file. The entire value must remain on one physical line:

```yaml
env.java.opts: -Djava.library.path=/usr/local/OmniStream:/opt/Dependency_library --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/sun.security.ssl=ALL-UNNAMED --add-exports java.base/sun.net.dns=ALL-UNNAMED --add-exports java.base/sun.net.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.math=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED
```

Press `Esc`, enter `:wq`, and press `Enter`.

Updated JVM options:

![Flink JVM options](../zh/figures/quick-start/flink-jvm-options.png)

### 9. Install Nexmark

```bash
cd /usr/local
wget --no-check-certificate https://github.com/nexmark/nexmark/releases/download/v0.2.0/nexmark-flink.tgz
tar -zxf nexmark-flink.tgz
mv nexmark-flink nexmark
chown -R root:root /usr/local/nexmark
rm -f nexmark-flink.tgz
cp /usr/local/nexmark/lib/nexmark-flink-0.2-SNAPSHOT.jar "$FLINK_HOME/lib/"
```

Open the Nexmark configuration script:

```bash
vi /usr/local/nexmark/bin/config.sh
```

Append the following configuration as one physical line:

```bash
export JAVA_TOOL_OPTIONS="-Djava.library.path=/usr/local/OmniStream:/opt/Dependency_library --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.security.ssl=ALL-UNNAMED --add-exports=java.base/sun.net.dns=ALL-UNNAMED --add-exports=java.base/sun.net.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED"
```

Press `Esc`, enter `:wq`, and press `Enter`.

Updated Nexmark JVM options:

![Nexmark JVM options](../zh/figures/quick-start/nexmark-jvm-options.png)

## Quick Start

### 1. Start Flink

```bash
source /etc/profile
"$FLINK_HOME/bin/start-cluster.sh"
```

Run `jps`. Flink has started successfully if the output contains `StandaloneSessionClusterEntrypoint` and `TaskManagerRunner`.

Flink processes:

![Flink processes](../zh/figures/quick-start/flink-processes.png)

### 2. Initialize Nexmark

```bash
bash /usr/local/nexmark/bin/setup_cluster.sh
```

Run `jps` again. Nexmark initialization has succeeded if the output contains `CpuMetricSender`.

Process check:

![Nexmark metric process](../zh/figures/quick-start/nexmark-process.png)

### 3. Run Q0

```bash
bash /usr/local/nexmark/bin/run_query.sh q0
```

Execution result:

![Nexmark Q0 result](../zh/figures/quick-start/nexmark-q0-result.png)

### 4. Verify OmniStream

```bash
grep "welcome to native" "$FLINK_HOME"/log/*
```

OmniStream is enabled if the log contains `OmniTask::DoRunInvoke welcome to native`.

Command output:

![OmniStream native log](../zh/figures/quick-start/omnistream-native-log.png)

## FAQ

### Flink processes exit immediately after startup

Run `ldd /usr/local/OmniStream/libtnel.so | grep "not found"` to check the native dependencies. If `libXext.so.6`, `libX11.so.6`, `libXrender.so.1`, `libXtst.so.6`, or `libXi.so.6` is missing, install the libraries:

```bash
yum install -y libXext libX11 libXrender libXtst libXi
```

### Flink reports that the configuration cannot be parsed

`flink-conf.yaml` uses the `key: value` format. `env.java.opts` and all its arguments must be on one physical line. Do not split the `--add-opens` or `--add-exports` arguments across lines.

### Q0 reports `{"jobs":[]}`

Run `grep -nE "ERROR|Exception|Caused by" /usr/local/nexmark/log/nexmark-flink.log`. If the log contains `InaccessibleObjectException`, verify that `JAVA_TOOL_OPTIONS` is configured in `/usr/local/nexmark/bin/config.sh`.

## More Information

- [Build Guide](./compile_guide.md)
- [Installation Guide](./installation_guide.md)
- [User Guide](./user_guide.md)
- [FAQ](./faq.md)

# Disclaimer

This repository is intended to participate in the Flink open source ecosystem and only provides Flink functionality extensions and performance improvements. Vulnerabilities and security issues in upstream software are handled by the corresponding communities under their security response processes.
