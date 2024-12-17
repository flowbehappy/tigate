# Workload Tool Usage Guide

This tool helps generate and manipulate test data for database performance testing.

## Prerequisites

- Go environment (1.16 or later recommended)

- Access to a target database (e.g., TiDB, MySQL)

## Building the Tool

```bash
go build -o ./workload
```

## Common Usage Scenarios

### 1. Sysbench-style Data Insertion

Insert test data using sysbench-compatible schema:

```bash
./workload -action insert \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name db1 \
    -total-row-count 100000000 \
    -table-count 1000 \
    -workload-type sysbench \
    -thread 32 \
    -batch-size 64
```

### 2. Large Row Update Workload

Update existing data with large row operations:

```bash
./workload -action update \
    -database-host 127.0.0.1 \
    -database-port 4000 \
    -database-db-name large \
    -total-row-count 100000000 \
    -table-count 1 \
    -large-ratio 0.1 \ 
    -workload-type large_row \
    -thread 16 \
    -batch-size 64 \
    -percentage-for-update 1
```

Additional parameters for update:

- large-ratio: Ratio of large rows in the dataset
- percentage-for-update: Percentage of rows to update

## Notes

- Ensure the database is properly configured and has the necessary permissions.
- Adjust the thread and batch-size parameters based on your needs.
