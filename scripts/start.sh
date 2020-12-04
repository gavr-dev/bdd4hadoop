#!/bin/sh

SOURCE="${BASH_SOURCE[0]}"
WORK_DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
JAR_PATH=$(ls $WORK_DIR/*.jar)

java -jar "$JAR_PATH" "$@"