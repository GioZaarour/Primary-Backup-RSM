# Primary-Backup Replicated State Machine in Go

This project is an excerpt from the Distributed Systems Course at USC. It represents my individual effort and may contain imperfections or bugs.

## Overview
This project develops a fault-tolerant key/value store service using primary/backup replication, supported by a view service for server role management. The system ensures consistent operation even with network partitions, maintaining a primary server and a backup for reliability. The project includes implementing RPCs, ensuring server role transitions, and managing data replication between primary and backup servers, aiming to address fault tolerance in distributed systems.

## Usage
To test the script, use the following commands in the project directory:

`go test`

For specific functions in the test file:

`go test -run FUNCTION_NAME`

To repeat a test:

`go test -run FUNCTION_NAME -count NUM_TIMES`

To filter warnings:

`go test | egrep -v "keyword1|keyword2|keyword3"`