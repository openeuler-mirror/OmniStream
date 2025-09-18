#!/bin/bash

##General
export C_INCLUDE_PATH=$C_INCLUDE_PATH:/usr/local/include
export CPLUS_INCLUDE_PATH=/usr/local/include
export LD_LIBRARY_PATH=/usr/local/lib
export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/lib

#
####
# Assume system has provided the following env

## java related
#export JAVA_HOME="/opt/bisheng-jdk1.8.0_342"
#PATH has contains the java and mvn
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib/jvm/java-11-openjdk-amd64/lib/server

### GCC related , system bundled, assume gcc 10
#export CC="/usr/bin/gcc"
#export CXX="/usr/bin/g++"


###LLVM related  LLVM assumed to be installed in /opt/llvm-15
#
export LLVM_INSTALL_DIR="/opt/llvm-15"
export LLVM_SO="/opt/llvm-15/lib/libLLVM-15.so"

export PATH=$LLVM_INSTALL_DIR/bin:$PATH
export C_INCLUDE_PATH=$C_INCLUDE_PATH:"$LLVM_INSTALL_DIR/include"
export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:"$LLVM_INSTALL_DIR/include"
export LIBRARY_PATH=$LIBRARY_PATH:"$LLVM_INSTALL_DIR/lib"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/llvm-15/lib

## huawei secure  assume it is at /opt/libboundscheck

export C_INCLUDE_PATH=$C_INCLUDE_PATH:/opt
export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:/opt
export LIBRARY_PATH=$LIBRARY_PATH:/opt/libboundscheck/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/libboundscheck/lib

## published
export OMNI_HOME=/repo/publish/omnihome
export LIBRARY_PATH=$LIBRARY_PATH:$OMNI_HOME/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$OMNI_HOME/lib

####
#mkdir build
#cd build
#
#make -j16

