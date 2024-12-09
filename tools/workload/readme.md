# Useage

```bash
./workload -action update \
    -database-host 127.0.0.1 -database-port 4000 -database-db-name large -skip-create-table \
    -total-row-count 100000000 -table-count 1 -large-ratio 0.1  -workload-type large_row \
    -qps 16 -rps 64 -percentage-for-update 1
```