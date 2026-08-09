#!/bin/bash
set -e

HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
OUT="$(pwd)/build"
rm -rf "$OUT"
mkdir -p "$OUT/classes" "$OUT/jars"

CP="$HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*"

echo "Compiling against Hadoop at: $HADOOP_HOME"
javac -cp "$CP" -d "$OUT/classes" src/*.java

for f in src/*.java; do
    name=$(basename "$f" .java)
    jar -cf "$OUT/jars/$name.jar" -C "$OUT/classes" .
done

echo
echo "Built:"
ls -lh "$OUT/jars"
