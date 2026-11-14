#!/bin/bash
cd "$(dirname "$0")"
javac -cp ".:../lib/*" YamlGeneratorController.java
java -cp ".:../lib/*" YamlGeneratorController