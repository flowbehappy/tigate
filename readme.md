TiGate
====

TiGate pulls change logs from TiDB clusters, and pushes them to downstream systems, including MySQL, TiDB, Kafka, Pulsar, Object Storages(e.g. S3), etc. TiGate is the next generation of [TiCDC](https://github.com/pingcap/tiflow). We build TiGate mainly to address some drawbacks of TiCDC and move forward.

* **Better scalability**. E.g. support over 1 million tables.
* **More efficiency**. Use less machine resource to support large volume.
* **Better maintainability**. E.g. simpler and human readable code, clear code module, and open to extensions.
* **Cloud native architecture**. We want to design a new architecture from the ground to support the cloud.


