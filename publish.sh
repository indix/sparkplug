#!/usr/bin/env bash

set -ex

sbt "project sparkplug" +publishSigned
sbt sonatypeReleaseAll

echo "Released"