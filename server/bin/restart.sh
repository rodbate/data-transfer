#!/usr/bin/env bash


CURRENT_PATH=$(dirname $0)
sh ${CURRENT_PATH}/stop.sh
sh ${CURRENT_PATH}/start.sh
