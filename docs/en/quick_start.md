# Quick Start
    
## Source Code Compilation

### Dependencies

<a name="table12473143919118"></a>
<table><thead align="left"><tr id="row154733396114"><th class="cellrowborder" valign="top" width="50%" id="mcps1.1.3.1.1"><p id="p5473143971116"><a name="p5473143971116"></a><a name="p5473143971116"></a>Software</p>
</th>
<th class="cellrowborder" valign="top" width="50%" id="mcps1.1.3.1.2"><p id="p1947393921119"><a name="p1947393921119"></a><a name="p1947393921119"></a>Version</p>
</th>
</tr>
</thead>
<tbody><tr id="row547315397112"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p11473139161116"><a name="p11473139161116"></a><a name="p11473139161116"></a>GCC</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p1447333916113"><a name="p1447333916113"></a><a name="p1447333916113"></a>10.3.1</p>
</td>
</tr>
<tr id="row19473939121120"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p847363915116"><a name="p847363915116"></a><a name="p847363915116"></a>CMake</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p15473239131119"><a name="p15473239131119"></a><a name="p15473239131119"></a>3.22.0</p>
</td>
</tr>
<tr id="row9474193915114"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p1247463911110"><a name="p1247463911110"></a><a name="p1247463911110"></a>JDK</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p1347433931113"><a name="p1347433931113"></a><a name="p1347433931113"></a>1.8.0_342</p>
</td>
</tr>
<tr id="row647473913118"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p3474153914116"><a name="p3474153914116"></a><a name="p3474153914116"></a>zlib</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p2474143961111"><a name="p2474143961111"></a><a name="p2474143961111"></a>1.2.8</p>
</td>
</tr>
<tr id="row12474183911120"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p19474173917114"><a name="p19474173917114"></a><a name="p19474173917114"></a>LLVM</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p9474143931119"><a name="p9474143931119"></a><a name="p9474143931119"></a>12.0.1</p>
</td>
</tr>
<tr id="row114741039161113"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p447410393119"><a name="p447410393119"></a><a name="p447410393119"></a>googletest</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p447433981120"><a name="p447433981120"></a><a name="p447433981120"></a>1.10.0</p>
</td>
</tr>
<tr id="row17474173911111"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p104741239191112"><a name="p104741239191112"></a><a name="p104741239191112"></a>jemalloc</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p18474183919116"><a name="p18474183919116"></a><a name="p18474183919116"></a>5.2.1</p>
</td>
</tr>
<tr id="row1474163919111"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p8474039101118"><a name="p8474039101118"></a><a name="p8474039101118"></a>nlomann json</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p3474739121110"><a name="p3474739121110"></a><a name="p3474739121110"></a>3.11.3</p>
</td>
</tr>
<tr id="row4474639131117"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p647453931110"><a name="p647453931110"></a><a name="p647453931110"></a>libboundscheck</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p147415395116"><a name="p147415395116"></a><a name="p147415395116"></a>V1.1.16</p>
</td>
</tr>
<tr id="row124741539151110"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p9474153919114"><a name="p9474153919114"></a><a name="p9474153919114"></a>OmniOperator</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p18474113921117"><a name="p18474113921117"></a><a name="p18474113921117"></a>20250630</p>
</td>
</tr>
<tr id="row547413394112"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p16474203916115"><a name="p16474203916115"></a><a name="p16474203916115"></a>snappy</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p1747443961114"><a name="p1747443961114"></a><a name="p1747443961114"></a>1.1.10</p>
</td>
</tr>
<tr id="row3474139111116"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p94749399115"><a name="p94749399115"></a><a name="p94749399115"></a>RocksDB</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p174741339121113"><a name="p174741339121113"></a><a name="p174741339121113"></a>8.11.4</p>
</td>
</tr>
<tr id="row154740392119"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p104741439191118"><a name="p104741439191118"></a><a name="p104741439191118"></a>BoostKit-kaccjson</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p1147423916119"><a name="p1147423916119"></a><a name="p1147423916119"></a>1.0.0</p>
</td>
</tr>
<tr id="row1147418391119"><td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.1 "><p id="p847513911117"><a name="p847513911117"></a><a name="p847513911117"></a>BoostKit-ksl</p>
</td>
<td class="cellrowborder" valign="top" width="50%" headers="mcps1.1.3.1.2 "><p id="p64751439141116"><a name="p64751439141116"></a><a name="p64751439141116"></a>2.5.1</p>
</td>
</tr>
</tbody>
</table>

### Compilation Commands

1. Before compiling OmniStream, run the following commands to compile the OmniAdaptor code:

    ```bash
    cd omnistream/omniop-flink-extension/omni-flink-bundle
    mvn clean package -DskipTests
    ```

2. Run the following commands to compile OmniStream:

    ```bash
    cd OmniStream/cpp
    mkdir build
    cd build
    cmake ..
    make install -j$PARALLELISM
    ```

    >**Note:**
    >`$PARALLELISM` indicates the specified parallelism degree.

## Environment Deployment

For details, see [Installation Guide](./installation_guide.md).

## Verification

1. Go to the `bin` directory in the Flink installation directory and start Flink.

    ```bash
    cd $FLINK_HOME/bin/ && ./start-cluster.sh
    ```

2. After calling sql-client, perform the test.

    ```bash
    ./sql-client.sh
    ```

3. In the CLI, enter the following content:

    ```bash
    SELECT 'Hello, Flink!';
    ```
    
    If the result is properly displayed, the installation is successful.

# Disclaimer

This code repository contributes to the Flink open-source project solely for performance optimization. It strictly adheres to the coding style and methods, as well as security design of the native open-source software. Any vulnerability and security issues of the software shall be resolved by the corresponding upstream communities according to their response mechanisms. Please pay attention to the notifications and version updates released by the upstream communities. The Kunpeng computing community does not assume any responsibility for software vulnerabilities and security issues.
