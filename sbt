#!/usr/bin/env bash

export BASE_DIR=$(cd "$(dirname $0)" 2>&1 >/dev/null ; pwd)

if [ ! -e $BASE_DIR/sbt-launch-*.jar ]; then
	curl -o "$BASE_DIR/sbt-launch-0.13.0.jar" http://repo.typesafe.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.13.0/sbt-launch.jar
fi
java -Xmx2400m -XX:MaxPermSize=350m -XX:ReservedCodeCacheSize=256m $EXTRA_ARGS -jar "$BASE_DIR"/sbt-launch-*.jar "$@"
