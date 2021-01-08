#!/bin/ksh

JBIN=/usr/java8_64/jre/bin/java
BDIR=/usr/local/proymon
LOGF=$BDIR/log/proymon.log

JARGS=''
JARGS=$JARGS' -Duser.language=en'
JARGS=$JARGS' -Djava.util.logging.config.file=logging.properties'
JARGS=$JARGS' -Dlog4j.configurationFile=log4j2.xml'
JARGS=$JARGS' -Djhefame.proymon.mongo=mongodb://proyman:87654321@mdb.hefame.es:27017/proyman'

if [ "$#" -eq 0 ]
then
        echo "Reiniciando recolector Proymon"
        kill $(ps -ef | grep proymon.jar | grep -v grep | awk '{print $2}')
        nohup $JBIN $JARGS -jar proymon.jar >> $LOGF 2>> $LOGF &
  exit 0
fi


if [ -f "$1" ]
then
  echo "Cargando lineas desde el fichero $1"
  $JBIN $JARGS -jar proymon.jar $1
  exit 0
fi


echo "USO: $0 [ fichero ]"
echo "  - Si no se especifica fichero, se lanza el recolector en modo demonio."
echo "  - Si se especifica un fichero que existe, se cargan las entradas de impresora del mismo."
