#!/usr/bin/env bash

mc alias set local http://localhost:9000 admin admin123

mc mb local/raw-logs
mc mb local/processed-logs

mc ls local