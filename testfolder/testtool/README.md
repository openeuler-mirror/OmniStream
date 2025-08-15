# OmniStream
import note: before using the tool, make sure you have python3 installed, please also set up flink properly and make sure OmniStream is patched. Also remember to add the enviroment variable
`  export FLINK_PERFORMANCE=false
`
some sample environment variables are shown below
##General

` export C_INCLUDE_PATH=$C_INCLUDE_PATH:/usr/local/include
  export CPLUS_INCLUDE_PATH=/usr/local/include
  export LD_LIBRARY_PATH=/usr/local/lib
  export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/lib
  export OMNISTREAM_HOME=/repo/codehub/OmniStream
`

#
####
# Assume system has provided the following env

## java related
#export JAVA_HOME="/opt/bisheng-jdk1.8.0_342"
#PATH has contains the java and mvn  
`export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib/jvm/java-11-openjdk-amd64/lib/server
`
#export JAVA_HOME="/opt/bisheng-jdk1.8.0_342"
#PATH has contains the java and mvn
`export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib/jvm/java-11-openjdk-amd64/lib/server
`

### GCC related , system bundled, assume gcc 10

#export CC="/usr/bin/gcc"
#export CXX="/usr/bin/g++"


###LLVM related  LLVM assumed to be installed in /opt/llvm-15
#

`
export LLVM_INSTALL_DIR="/opt/llvm-15"
export LLVM_SO="/opt/llvm-15/lib/libLLVM-15.so"
export PATH=$LLVM_INSTALL_DIR/bin:$PATH
export C_INCLUDE_PATH=$C_INCLUDE_PATH:"$LLVM_INSTALL_DIR/include"
export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:"$LLVM_INSTALL_DIR/include"
export LIBRARY_PATH=$LIBRARY_PATH:"$LLVM_INSTALL_DIR/lib"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/llvm-15/lib
`

## huawei secure  assume it is at /opt/huawei_secure_c
`
export C_INCLUDE_PATH=$C_INCLUDE_PATH:/opt
export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:/opt
export LIBRARY_PATH=$LIBRARY_PATH:/opt/huawei_secure_c/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/huawei_secure_c/lib
`

## published
`
export OMNI_HOME=/repo/publish/omnihome
export LIBRARY_PATH=$LIBRARY_PATH:$OMNI_HOME/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$OMNI_HOME/lib
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
`

####
#mkdir build
#cd build
#
#make -j16
`
export FLINK_HOME=/repo/codehub/flink-1.16.3
export FLINK_PERFORMANCE=false
`


<span style="font-size:16px;">Instruction of how to use the testing tool
</span>1.
export the enviroment variables of ${FLINK_HOME} and ${OMNISTREAM_HOME} which represent the  paths of the flink and omnistream project

additionally, also export ${QUERY_LOGS} where you can find the log of each query and output results

for example:
`  export FLINK_HOME=/repo/codehub/flink-1.16.3
`  
`export OMNISTREAM_HOME=/repo/codehub/OmniStream
`
`export QUERY_LOGS=/repo/codehub
`
2.
go the folder testtool under OMNISTREAM_HOME` /OmniStream/testtool `, and go to the entrypoint fold  `/OmniStream/testtool/entrypoint`
run the script runOmnistream.sh using the commend `./runOmnistream.sh`.

3.
the output is `please enter the  query ID (0 to 22)` asking the user to input a serial of numbers ranging from 0 to 22. The input format should be number,number,number ......, each number must be seperated by comma. Entering a space means running all queries by default
