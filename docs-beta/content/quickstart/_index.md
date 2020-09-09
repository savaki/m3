---
linktitle: "Quickstart"
date: 2020-04-21T20:46:17-04:00
weight: 3
---

<!-- TODO: Fix dates. Cross-platform date generation is a pain, so maybe use Docker locally? See what Netlify supports, or maybe there is a Hugo variable, or create a shortcode -->

# Creating a Single Node M3DB Cluster

This guide shows how to install and configure M3DB, create a single-node cluster, and read and write metrics to it.

{{% notice warning %}}
Deploying a single-node M3DB cluster is a great way to experiment with M3DB and get an idea of what it has to offer, but is not designed for production use. To run M3DB in clustered mode, with a separate M3Coordinator [read the clustered mode guide](cluster_hard_way.md).
{{% /notice %}}

## Prerequisites

-   **Docker**: You don't need [Docker](https://www.docker.com/get-started) to run M3DB, but it is the simplest and quickest way.
    -   If you use Docker Desktop, we recommend the following minimum _Resources_ settings.
        -   _CPUs_: 2
        -   _Memory_: 8GB
        -   _Swap_: 1GB
        -   _Disk image size_: 16GB
-   **JQ**: This example uses [jq](https://stedolan.github.io/jq/) to format the output of API calls. It is not essential for using M3DB.
-   **curl**: This example uses curl for communicating with M3DB endpoints. You can also use alternatives such as [Wget](https://www.gnu.org/software/wget/) and [HTTPie](https://httpie.org/).

## Start Docker Container

By default the official M3DB Docker image configures a single M3DB instance as one binary containing:

-   An M3DB storage instance for time series storage. It includes an embedded tag-based metrics index and an etcd server for storing the cluster topology and runtime configuration.
-   A coordinator instance for writing and querying tagged metrics, as well as managing cluster topology and runtime configuration.

The Docker container exposes three ports:

-   `7201` to manage the cluster topology, you make most API calls to this endpoint
-   `7203` for Prometheus to scrape the metrics produced by M3DB and M3Coordinator
-   `9003` to read and write metrics

The command below creates a persistent data directory on the host operating system to maintain durability and persistence between container restarts.

{{< tabs name="start_container" >}}
{{% tab name="Command" %}}

```shell
docker pull quay.io/m3db/m3dbnode:latest
docker run -p 7201:7201 -p 7203:7203 -p 9003:9003 --name m3db -v $(pwd)/m3db_data:/var/lib/m3db quay.io/m3db/m3dbnode:latest
```

{{% /tab %}}
{{% tab name="Output" %}}

<!-- TODO: Perfect image, pref with terminalizer -->

![Docker pull and run](/docker-install.gif)

{{% /tab %}}
{{< /tabs >}}

{{% notice info %}}
When running the command above on Docker for Mac, Docker for Windows, and some Linux distributions you may see errors about settings not being at recommended values. Unless you intend to run M3DB in production on macOS or Windows, you can ignore these warnings.
{{% /notice %}}

## Configuration

The single-node cluster Docker image uses this [sample configuration file](https://github.com/m3db/m3/blob/master/src/dbnode/config/m3dbnode-local-etcd.yml).

The file groups configuration into `coordinator` or `db` sections that represent the `M3Coordinator` and `M3DB` instances part of single-node cluster.

{{% notice tip %}}
You can find more information on configuring M3DB in the [operational guides section](/operational_guide/).
{{% /notice %}}

## Organizing Data with Placements and Namespaces

<!-- TODO: Find an image -->

M3DB organizes data in similar ways to other databases but adds extra concepts that reflect the time series metrics typically stored with M3DB.

Every cluster has **one** {{< glossary_tooltip text="placement" term_id="placement" >}} that maps cluster shard replicas to nodes in the cluster.

A cluster can have **0 or more** {{< glossary_tooltip text="namespaces" term_id="namespace" >}} that are similar conceptually to tables in other databases, and each node serves every namespace for the shards it owns.

<!-- TODO: Image -->

For example, if the cluster placement states that node A owns shards 1, 2, and 3, then node A owns shards 1, 2, 3 for all configured namespaces in the cluster. Each namespace has its own configuration options, including a name and retention time for the data.

## Create a Placement and Namespace

This quickstart uses the _{{% apiendpoint %}}database/create_ endpoint that creates a namespace, and the placement if it doesn't already exist based on the `type` argument.

You can create [placements](https://docs.m3db.io/operational_guide/placement_configuration/) and [namespaces](https://docs.m3db.io/operational_guide/namespace_configuration/#advanced-hard-way) separately if you need more control over their settings.

The `namespaceName` argument must match the namespace in the `local` section of the `M3Coordinator` YAML configuration. If you [add any namespaces](../operational_guide/namespace_configuration.md) you also need to add them to the `local` section of `M3Coordinator`'s YAML config.

{{< tabs name="create_placement_namespace" >}}
{{% tab name="Command" %}}

```json
curl -X POST {{% apiendpoint %}}database/create -d '{
  "type": "local",
  "namespaceName": "default",
  "retentionTime": "12h"
}'
```

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
  "namespace": {
    "registry": {
      "namespaces": {
        "default": {
          "bootstrapEnabled": true,
          "flushEnabled": true,
          "writesToCommitLog": true,
          "cleanupEnabled": true,
          "repairEnabled": false,
          "retentionOptions": {
            "retentionPeriodNanos": "43200000000000",
            "blockSizeNanos": "1800000000000",
            "bufferFutureNanos": "120000000000",
            "bufferPastNanos": "600000000000",
            "blockDataExpiry": true,
            "blockDataExpiryAfterNotAccessPeriodNanos": "300000000000",
            "futureRetentionPeriodNanos": "0"
          },
          "snapshotEnabled": true,
          "indexOptions": {
            "enabled": true,
            "blockSizeNanos": "1800000000000"
          },
          "schemaOptions": null,
          "coldWritesEnabled": false,
          "runtimeOptions": null
        }
      }
    }
  },
  "placement": {
    "placement": {
      "instances": {
        "m3db_local": {
          "id": "m3db_local",
          "isolationGroup": "local",
          "zone": "embedded",
          "weight": 1,
          "endpoint": "127.0.0.1:9000",
          "shards": [
            {
              "id": 0,
              "state": "INITIALIZING",
              "sourceId": "",
              "cutoverNanos": "0",
              "cutoffNanos": "0"
            },
            …
            {
              "id": 63,
              "state": "INITIALIZING",
              "sourceId": "",
              "cutoverNanos": "0",
              "cutoffNanos": "0"
            }
          ],
          "shardSetId": 0,
          "hostname": "localhost",
          "port": 9000,
          "metadata": {
            "debugPort": 0
          }
        }
      },
      "replicaFactor": 1,
      "numShards": 64,
      "isSharded": true,
      "cutoverTime": "0",
      "isMirrored": false,
      "maxShardSetId": 0
    },
    "version": 0
  }
}
```

{{< /tab >}}
{{< /tabs >}}

Placement initialization can take a minute or two. Once all the shards have the `AVAILABLE` state, the node has finished bootstrapping, and you should see the following messages in the node console output.

<!-- TODO: Fix these timestamps -->

```shell
{"level":"info","ts":1598367624.0117292,"msg":"bootstrap marking all shards as bootstrapped","namespace":"default","namespace":"default","numShards":64}
{"level":"info","ts":1598367624.0301404,"msg":"bootstrap index with bootstrapped index segments","namespace":"default","numIndexBlocks":0}
{"level":"info","ts":1598367624.0301914,"msg":"bootstrap success","numShards":64,"bootstrapDuration":0.049208827}
{"level":"info","ts":1598367624.03023,"msg":"bootstrapped"}
```

You can check on the status by calling the _placement_ endpoint:

{{< tabs name="check_placement" >}}
{{% tab name="Command" %}}

```shell
curl {{% apiendpoint %}}placement | jq .
```

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
  "placement": {
    "instances": {
      "m3db_local": {
        "id": "m3db_local",
        "isolationGroup": "local",
        "zone": "embedded",
        "weight": 1,
        "endpoint": "127.0.0.1:9000",
        "shards": [
          {
            "id": 0,
            "state": "AVAILABLE",
            "sourceId": "",
            "cutoverNanos": "0",
            "cutoffNanos": "0"
          },
          …
          {
            "id": 63,
            "state": "AVAILABLE",
            "sourceId": "",
            "cutoverNanos": "0",
            "cutoffNanos": "0"
          }
        ],
        "shardSetId": 0,
        "hostname": "localhost",
        "port": 9000,
        "metadata": {
          "debugPort": 0
        }
      }
    },
    "replicaFactor": 1,
    "numShards": 64,
    "isSharded": true,
    "cutoverTime": "0",
    "isMirrored": false,
    "maxShardSetId": 0
  },
  "version": 2
}
```

{{% /tab %}}
{{< /tabs >}}

{{% notice tip %}}
[Read more about the bootstrapping process](https://docs.m3db.io/operational_guide/bootstrapping_crash_recovery/).
{{% /notice %}}

## Writing and Querying Metrics

M3DB supports two query engines:

-   **Prometheus (default)** - robust and commonly-used query language for metrics
-   **M3 Query Engine** - in development higher-performance query engine

### Writing Metrics

As M3DB is a time series database (TSDB), metric data consists of a value, a timestamp, and tags to bring context and meaning to the metric.

You can write metrics using one of two endpoints:

-   _[/prom/remote/write](/m3coordinator/api/remote/)_ - Write a Prometheus remote write query to M3DB with a binary snappy compressed Prometheus WriteRequest protobuf message.
-   _/json/write_ - Write a JSON payload of metrics data.

For this quickstart, use the _{{% apiendpoint %}}json/write_ endpoint to write a tagged metric to M3DB with the following data in the request body, all fields are required:

-   `tags`: An object of at least one `name`/`value` pairs
-   `timestamp`: The UNIX timestamp for the data
-   `value`: The value for the data, can be of any type

{{% notice tip %}}
The examples below use `__name__` as the name for one of the tags, which is a Prometheus reserved tag that allows you to query metrics using the value of the tag to filter results.
{{% /notice %}}

{{% notice tip %}}
Label names may contain ASCII letters, numbers, underscores, and Unicode characters. They must match the regex `[a-zA-Z_][a-zA-Z0-9_]*`. Label names beginning with `__` are reserved for internal use. [Read more in the Prometheus documentation](https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels).
{{% /notice %}}

{{< tabs name="write_metrics" >}}
{{< tab name="Command 1" >}}

{{% codeinclude file="/static/quickstart/write-metrics-1.sh" language="shell" %}}

{{< /tab >}}
{{< tab name="Command 2" >}}

{{% codeinclude file="/static/quickstart/write-metrics-2.sh" language="shell" %}}

{{< /tab >}}
{{< tab name="Command 3" >}}

{{% codeinclude file="/static/quickstart/write-metrics-3.sh" language="shell" %}}

{{< /tab >}}
{{< /tabs >}}

### Querying metrics

As this quickstart uses Prometheus as the query engine, you have access to [all the features of PromQL queries](https://prometheus.io/docs/prometheus/latest/querying/basics/). 

To query metrics, use the _{{% apiendpoint %}}query_range_ endpoint with the following data in the request body, all fields are required:

-   `query`: A PromQL query
-   `start`: Timestamp in `RFC3339Nano` of start range for results
-   `end`: Timestamp in `RFC3339Nano` of end range for results
-   `step`: A duration or float of the query resolution, the interval between results in the timespan between `start` and `end`.

Below are some examples using the metrics written above.

#### Return results in past 45 seconds

{{< tabs name="example_promql_regex" >}}
{{% tab name="Linux" %}}

{{% notice tip %}}
You need to encode the query below.
{{% /notice %}}

```shell
curl -X "POST" "{{% apiendpoint %}}query_range?
  query=third_avenue&
  start=$(date "+%s" -d "45 seconds ago")&
  end=$(date "+%s")&
  step=5s" | jq .
```

{{% /tab %}}
{{% tab name="macOS/BSD" %}}

{{% notice tip %}}
You need to encode the query below.
{{% /notice %}}

```shell
curl -X "POST" "{{% apiendpoint %}}query_range?
  query=third_avenue&
  start=$(date -v -45S "+%s")&
  end=$(date "+%s")&
  step=5s" | jq .
```

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "__name__": "third_avenue",
          "checkout": "1",
          "city": "new_york"
        },
        "values": [
          [
            {{% now %}},
            "3347.26"
          ],
          [
            {{% now %}},
            "5347.26"
          ],
          [
            {{% now %}},
            "7347.26"
          ]
        ]
      }
    ]
  }
}
```

{{% /tab %}}
{{< /tabs >}}

#### Values above a certain number

{{< tabs name="example_promql_range" >}}
{{% tab name="Linux" %}}

{{% notice tip %}}
You need to encode the query below.
{{% /notice %}}

```shell
curl -X "POST" "{{% apiendpoint %}}query_range?
  query=third_avenue > 6000
  start=$(date "+%s" -d "45 seconds ago")&
  end=$(date "+%s")&
  step=5s" | jq .
```

{{% /tab %}}
{{% tab name="macOS/BSD" %}}

{{% notice tip %}}
You need to encode the query below.
{{% /notice %}}

```shell
curl -X "POST" "{{% apiendpoint %}}query_range?
  query=third_avenue > 6000
  start=$(date -v -45S "+%s")&
  end=$(date "+%s")&
  step=5s" | jq .
```

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "__name__": "third_avenue",
          "checkout": "1",
          "city": "new_york"
        },
        "values": [
          [
            {{% now %}},
            "7347.26"
          ]
        ]
      }
    ]
  }
}
```

{{% /tab %}}
{{< /tabs >}}

## Next Steps

This quickstart covered getting a single-node M3DB cluster running, and writing and querying metrics to the cluster. Some next steps are:

-   one
-   two