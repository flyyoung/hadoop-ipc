#########################################################################
# File Name: test-start-server.sh
# Author: Jiang
# Mail: rain_man_happy@126.com
# Created Time: 2013年06月24日 星期一 10时03分44秒
# Description: This script is for starting a RPC-Server.
#########################################################################
#!/bin/bash

# initialized with default system path
CLASS_PATH=$PATH
CURRENT_DIR=`pwd`
LOG=$CURRENT_DIR/log/server.log

RUN_JAVA=java
JAVA_OPTS=""
PORT=4453
MAIN_CLASS=examples.InstancedClient

init()
{
  for jar in `ls $CURRENT_DIR/lib/*.jar `
  do
    CLASS_PATH=$CLASS_PATH:$jar
  done
}

run()
{
  init

  $RUN_JAVA $JAVA_OPTS \
 	-classpath $CLASS_PATH \
 	$MAIN_CLASS 
}


run
