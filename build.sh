#!/usr/bin/env bash
# Compile and run all tests with nothing but a JDK (17+).   Usage: ./build.sh
set -euo pipefail
cd "$(dirname "$0")"
rm -rf out && mkdir -p out/main out/test
javac -Xlint:all -d out/main $(find src/main -name '*.java')
if [ "${1:-}" = "--no-tests" ]; then exit 0; fi
javac -Xlint:all -cp out/main -d out/test $(find src/test -name '*.java')
java -cp out/main:out/test dev.rakshit.kv.testkit.Runner \
  dev.rakshit.kv.BasicTests dev.rakshit.kv.RecoveryTests dev.rakshit.kv.CrashInjectionTests dev.rakshit.kv.ComponentTests
