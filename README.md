<h1 align="center">
  <b>Driftwood</b>
  <br>
  <a href="https://opensource.org/licenses/MIT">
    <img src="https://img.shields.io/badge/License-MIT-green.svg">
  </a>
  <a href="https://crates.io/crates/dbsp">
    <img alt="crates.io" src="https://img.shields.io/crates/v/dbsp.svg">
  </a>
</h1>

<p align="center">
  <em><b>Driftwood</b></em> (formerly <b>Feldera Core</b>) is a high-throughput, low-overhead distributed incremental computation and transactional streaming database engine. Driftwood evaluates arbitrary SQL programs incrementally using multiset $\mathbb{Z}$-sets (DBSP), integrated with lightweight distributed consensus (OmniPaxos configuration & monotonic epoch fencing), partitioned chain replication, and Calvin-style deterministic transactions.
</p>

---

## 📌 Initial Fork & Lineage Permalink

Driftwood originated as an open-core decoupling and high-availability extension of Feldera. To inspect the initial baseline fork before the Driftwood rebase:

- **Initial Fork Branch**: `mit_core_refactor`
- **Initial Fork Commit Permalink**: [`63e5b3f91957d9b61bc5cd9eeb4872746eca2cff`](https://github.com/xenacool/feldera/tree/63e5b3f91957d9b61bc5cd9eeb4872746eca2cff)
- **Upstream Baseline**: [Feldera Open Core](https://github.com/feldera/feldera)

---

## 🔄 Feldera Compatibility & Architectural Evolution

Driftwood maintains full functional and semantic compatibility with existing Feldera SQL queries, pipeline definitions, and data adapters while removing proprietary operational friction:

| Subsystem | Upstream Feldera Legacy Behavior | Driftwood Clean-Room Architecture | Compatibility Impact |
| :--- | :--- | :--- | :--- |
| **Licensing & Entitlements** | License check timers (`license.rs`), trial expiration warnings, and `EnterpriseFeature` error gates on checkpoints/suspend. | **Pure MIT/Apache-2.0 Open Core**: Zero licensing shims, zero enterprise feature flags, and unrestricted checkpointing/lifecycle control. | **100% Compatible Drop-in**: All API calls succeed without enterprise license keys or trial limits. |
| **Telemetry & Egress** | Background telemetry beacons (`feldera-cloud1-client`, PostHog, telemetry reporting threads). | **Zero Egress Privacy**: Telemetry excised entirely; operational metrics exported exclusively via standard Prometheus exposition (`/metrics`). | **Non-Breaking**: Completely transparent, with reduced background CPU overhead and zero network phone-home. |
| **Control Plane Storage** | Relational metadata storage bound to PostgreSQL (`StoragePostgres`, `deadpool-postgres`, `refinery` migrations). | **Self-Hosted DBSP Relational Metadata**: Metadata modeled directly as multiset $\mathbb{Z}$-sets with snapshots committed to `object_store`. | **Zero External DBMS**: No PostgreSQL instance required to run the pipeline manager or cluster controller. |
| **Checkpoint Replication** | Ad-hoc S3 polling loop (`continuous_pull`, `pull_and_gc`) coupled to closed-source plugins (`sync-checkpoint`). | **Consensus-Driven Standby & Chain Replication**: Partitioned chain replication for WALs + OmniPaxos epoch fencing ($\mathcal{E}_k$) + Apache Arrow `object_store`. | **Superior Durability**: Eliminates polling race hazards, GC sync conflicts, and split-brain dual-primary corruption. |
| **Transactional Front-End** | HTTP/REST batch endpoints without cross-partition strict serializability. | **Partitioned PostgreSQL Wire Interface + Calvin Transactions**: Deterministic lock manager (DLM) delivering Strict Serializability (Strict-1SR). | **PostgreSQL Parity**: Connect standard `psql`, JDBC, or async PostgreSQL drivers directly. |

---

## 🔥 Incremental Computation Engine

Our approach to incremental computation is simple. A Driftwood `pipeline` is a set of SQL tables and views. Views can be deeply nested. Users start, stop or pause pipelines to manage and advance a computation.

Pipelines continuously process **changes**, which are any number of inserts, updates or deletes to a set of tables. When the pipeline receives changes, Driftwood **incrementally** updates all the views by only looking at the changes and completely avoids recomputing over older data.

While a pipeline is running, users can inspect the results of the views at any time or stream incremental updates downstream with sub-millisecond latencies.

## 🎯 Defining Features

1. **Full SQL support and more.** Evaluates full SQL syntax and semantics incrementally: joins, aggregates, `GROUP BY`, correlated subqueries, window functions, complex nested types, time-series watermarks, UDFs, and recursive queries.
2. **Deterministic Calvin Transactions.** PostgreSQL v3.0 wire protocol frontend with deterministic lock sequencing, achieving strict serializability across distributed partitions without distributed 2PC abort cascades.
3. **High-Availability Consensus & Partitioned Chain Replication.** OmniPaxos ensures monotonic epoch fencing ($\mathcal{E}_k$) and split-brain immunity while high-volume data streams replicate along partitioned chains ($\text{Head} \rightarrow \text{Tail}$) with sub-millisecond tail acknowledgments.
4. **Native Object Storage Persistence.** Tiered persistence backed by Apache Arrow `object_store` (S3, MinIO, GCS, Azure Blob, and local POSIX NVMe storage) using immutable, content-addressed SST runs and atomic manifests.
5. **Zero External DBMS Dependencies.** Control plane metadata is evaluated inside DBSP as incremental materialized views, eliminating external PostgreSQL database operational requirements.
6. **Extensive Connectors.** Connects to Kafka, CDC streams (Postgres WAL), Apache Avro, Nexmark, Iceberg, Delta Lake, HTTP, S3, and more.

## 💻 Distributed Architecture

```
+-----------------------------------------------------------------------------------+
|                        Target Distributed Architecture                            |
|                                                                                   |
|  +-----------------------------------------------------------------------------+  |
|  |             Consensus Layer: OmniPaxos (Config, Topology, Fencing)          |  |
|  +-----------------------------------------------------------------------------+  |
|                   |                                            |                  |
|                   | Monotonic Epoch Fencing (\mathcal{E}_k)    |                  |
|                   v                                            v                  |
|  +---------------------------------+          +---------------------------------+  |
|  | Partition 0 Replication Chain   |          | Partition 1 Replication Chain   |  |
|  | [Head 0] -> [Node 0B] -> [Tail0]|          | [Head 1] -> [Node 1B] -> [Tail1]|  |
|  | - Streaming Data Log (WAL)      |          | - Streaming Data Log (WAL)      |  |
|  | - DBSP Circuit Incremental Step |          | - DBSP Circuit Incremental Step |  |
|  | - Calvin Deterministic Locks    |          | - Calvin Deterministic Locks    |  |
|  +---------------------------------+          +---------------------------------+  |
|                   \                                            /                  |
|                    \------- Stage Immutable Batch SSTs -------/                   |
|                                         v                                         |
|  +-----------------------------------------------------------------------------+  |
|  |        Unified `object_store` (S3 / GCS / Azure / MinIO / Local POSIX)      |  |
|  |   - Immutable Run SSTs (`<uuid>.dbsp`) & Checkpoint Manifests (`epoch_k.json`)|
|  +-----------------------------------------------------------------------------+  |
+-----------------------------------------------------------------------------------+
```

---

## ⚡️ Quick start with Docker

First, make sure you have [Docker](https://docs.docker.com/) installed. Then run the
following command:

```text
docker run -p 8080:8080 --tty --rm -it images.feldera.com/feldera/pipeline-manager:latest
```

Once the container image downloads and you see the Feldera logo on your terminal, visit
the WebConsole at [http://localhost:8080](http://localhost:8080).
We suggest going through our [tutorial](https://docs.feldera.com/tutorials/basics/) next.

We also have instructions to run Feldera using [Docker Compose](https://docs.feldera.com/get-started),
if you'd like to experiment with Kafka and other auxiliary services.

## ⚙️ Running Feldera from sources

To run Feldera from sources, ensure at least 6 GB of free space in the sources directory and an additional 7 GB in your home directory, then install the required dependencies:

- [Rust tool chain](https://www.rust-lang.org/tools/install)
- C and C++ compiler toolchain (e.g., gcc, g++)
- cmake
- libssl-dev
- libsasl2-dev
- zlib1g-dev
- libzstd-dev
- golang-go (only to build with `--features fips`, which compiles aws-lc-fips-sys from source; a default build does not need it)
- pkg-config
- clang
- graphviz
- Java Development Kit (JDK), version 19 or newer (21 is recommended)
- maven
- Python 3.10 (for the [Python SDK](https://docs.feldera.com/python/) and integration tests)
- [Bun](https://bun.sh/docs/installation)
- [nodejs v20](https://github.com/nodesource/distributions/blob/master/DEV_README.md)

On MacOS, after installing the Rust tool chain, the remaining dependencies can be installed with:
```
xcode-select --install
```
for Xcode tools that includes clang, and
```
brew install cmake openssl cyrus-sasl zlib zstd go pkg-config graphviz openjdk@21 maven python@3.10 oven-sh/bun/bun node@20
```
for the rest.

The Kafka connectors link librdkafka dynamically, so it has to be installed before the workspace will build:

```
./scripts/install-librdkafka.sh
```

The script builds librdkafka against AWS-LC, which is what keeps Kafka TLS on the same cryptographic implementation as the rest of the system. Distribution packages are built against OpenSSL and are usually older than the version the `rdkafka-sys` crate requires, so installing one of those is not equivalent. Run the script again after a `rdkafka` version bump; it reads the version it needs from `Cargo.lock`. Set `PREFIX` to install somewhere other than `/usr/local`, in which case `PKG_CONFIG_PATH` has to point at `$PREFIX/lib/pkgconfig`.

After that, the first step is to build the SQL compiler:

```
cd sql-to-dbsp-compiler
./build.sh
```

Next, from the repository root, run the pipeline-manager:

```
cargo run --bin=pipeline-manager
```

As with the Docker instructions above, you can now visit
[http://localhost:8080](http://localhost:8080) on your browser to see the
Feldera WebConsole.

## 📖 Documentation

To learn more about Feldera Platform, we recommend going through the
[documentation](https://docs.feldera.com).

* [Getting started](https://docs.feldera.com/get-started)
* [Feldera basics](https://docs.feldera.com/tutorials/basics/)
* [Tutorials](https://docs.feldera.com/tutorials)
* [SQL reference](https://docs.feldera.com/sql/)
* [API reference](https://docs.feldera.com/api)
* [Python SDK](https://docs.feldera.com/python/)

## 🤖 Benchmarks

Feldera is generally [faster and uses less memory](https://www.feldera.com/blog/nexmark-vs-flink)
than systems like stream processors.

<p float="left" align="middle">
  <img alt="Nexmark throughput in events per second by query, comparing Feldera in-memory, Feldera with storage, and Flink" src="https://cdn.sanity.io/images/nlte859i/production/c80a9d592fb6f6e4cf2c7a665add24da65998123-1740x493.png?auto=format&fit=max&w=1740" width="100%">
</p>

## 👍 Contributing

The software in this repository is governed by an open-source license.
We welcome contributions. Here are some [guidelines](CONTRIBUTING.md).

## 🎓 Theory

Feldera Platform achieves its objectives by building on a solid mathematical
foundation. The formal model that underpins our system, called DBSP, is
described in the accompanying paper:

- [Budiu, Chajed, McSherry, Ryzhyk, Tannen. DBSP: Automatic
  Incremental View Maintenance for Rich Query Languages, Conference on
  Very Large Databases, August 2023, Vancouver,
  Canada](https://docs.feldera.com/vldb23.pdf)

- Here is [a presentation about DBSP](https://www.youtube.com/watch?v=iT4k5DCnvPU) at the 2023
  Apache Calcite Meetup.

The model provides two things:

1. **Semantics.** DBSP defines a formal language of streaming operators and
   queries built out of these operators, and precisely specifies how these queries
   must transform input streams to output streams.

1. **Algorithm.** DBSP also gives an algorithm that takes an arbitrary query and
   generates an incremental dataflow program that implements this query correctly (in accordance
   with its formal semantics) and efficiently. Efficiency here means, in a
   nutshell, that the cost of processing a set of input events is proportional to
   the size of the input rather than the entire state of the database.

