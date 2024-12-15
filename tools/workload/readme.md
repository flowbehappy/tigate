# Useage

```bash
go build -o ./workload  && ./workload -action update \
    -database-host 127.0.0.1 -database-port 4000 -database-db-name large \
    -total-row-count 100000000 -table-count 1 -large-ratio 0.1  -workload-type large_row \
    -thread 16 -batch-size 64 -percentage-for-update 1
```