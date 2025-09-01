# Throttling in OpenFGA

OpenFGA provides sophisticated throttling mechanisms to manage resource usage and prevent excessive workloads that could degrade system performance or cause outages. This document explains how throttling works in OpenFGA and how to configure it for your deployment. 

In most scenarios, you will keep throttling disabled, which is the default value. However, if your OpenFGA service is invoked by multiple applications developed indepedently, you might want to slow down expensive requests so the overall performance is not impacted.

## Overview

OpenFGA implements two complementary types of throttling:

1. **Dispatch Throttling**: Controls the number of recursive sub-operations (dispatches) during complex queries
2. **Datastore Throttling**: Limits concurrent database operations to prevent database overload

Both mechanisms help protect your OpenFGA deployment from resource exhaustion while maintaining good performance for typical workloads.

## Understanding Dispatches

Before diving into throttling configuration, it's important to understand what "dispatches" are in OpenFGA:

- A **dispatch** is a recursive sub-operation that occurs during complex authorization checks
- For example, checking if `user:alice` has `viewer` access to `document:readme` might require:
 1. Checking direct relationships (1 dispatch)
 2. Checking group memberships (additional dispatches per group)
 3. Checking nested group relationships (more dispatches)

Complex authorization models with deep hierarchies can generate many dispatches, potentially consuming significant resources.

## Dispatch Throttling

Dispatch throttling prioritizes requests with fewer dispatches over requests with more dispatches. When a request's dispatch count exceeds the configured threshold, it's placed in a throttling queue and processed at a controlled rate.

### How Dispatch Throttling Works

1. **Normal Processing**: Requests with dispatch count ≤ threshold are processed immediately
2. **Throttled Processing**: Requests exceeding the threshold are queued and processed periodically based on the configured frequency
3. **Gradual Throttling**: This allows expensive queries to complete while protecting system resources

### Configuration Options

Dispatch throttling can be configured separately for each API operation:

#### Check API Throttling

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `OPENFGA_CHECK_DISPATCH_THROTTLING_ENABLED` | `--check-dispatch-throttling-enabled` | `false` | Enable dispatch throttling for Check requests |
| `OPENFGA_CHECK_DISPATCH_THROTTLING_THRESHOLD` | `--check-dispatch-throttling-threshold` | `100` | Number of dispatches before throttling kicks in |
| `OPENFGA_CHECK_DISPATCH_THROTTLING_MAX_THRESHOLD` | `--check-dispatch-throttling-max-threshold` | `0` | Maximum threshold allowed for per-request overrides (0 = use default threshold) |
| `OPENFGA_CHECK_DISPATCH_THROTTLING_FREQUENCY` | `--check-dispatch-throttling-frequency` | `10µs` | How often throttled requests are processed |

#### ListObjects API Throttling

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `OPENFGA_LIST_OBJECTS_DISPATCH_THROTTLING_ENABLED` | `--listObjects-dispatch-throttling-enabled` | `false` | Enable dispatch throttling for ListObjects requests |
| `OPENFGA_LIST_OBJECTS_DISPATCH_THROTTLING_THRESHOLD` | `--listObjects-dispatch-throttling-threshold` | `100` | Number of dispatches before throttling kicks in |
| `OPENFGA_LIST_OBJECTS_DISPATCH_THROTTLING_MAX_THRESHOLD` | `--listObjects-dispatch-throttling-max-threshold` | `0` | Maximum threshold allowed for per-request overrides (0 = use default threshold) |
| `OPENFGA_LIST_OBJECTS_DISPATCH_THROTTLING_FREQUENCY` | `--listObjects-dispatch-throttling-frequency` | `10µs` | How often throttled requests are processed |

#### ListUsers API Throttling

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `OPENFGA_LIST_USERS_DISPATCH_THROTTLING_ENABLED` | `--listUsers-dispatch-throttling-enabled` | `false` | Enable dispatch throttling for ListUsers requests |
| `OPENFGA_LIST_USERS_DISPATCH_THROTTLING_THRESHOLD` | `--listUsers-dispatch-throttling-threshold` | `100` | Number of dispatches before throttling kicks in |
| `OPENFGA_LIST_USERS_DISPATCH_THROTTLING_MAX_THRESHOLD` | `--listUsers-dispatch-throttling-max-threshold` | `0` | Maximum threshold allowed for per-request overrides (0 = use default threshold) |
| `OPENFGA_LIST_USERS_DISPATCH_THROTTLING_FREQUENCY` | `--listUsers-dispatch-throttling-frequency` | `10µs` | How often throttled requests are processed |


The `*MAX_THRESHOLD` settings should keep the default value unless you are using OpenFGA as a library and plan to set the throttling per request. In that case, the `MAX_THRESHOLD` will act as an upper limit regardless of the value specified in the request.

## Datastore Throttling

Datastore throttling limits the number of concurrent database operations to prevent overwhelming your database. This is particularly important for high-throughput scenarios where many requests could cause database connection exhaustion or performance degradation.

### How Datastore Throttling Works

1. **Concurrent Operations Limit**: Controls how many database operations can run simultaneously
2. **Queue Management**: Excess requests wait in a queue until resources become available
3. **Timeout Protection**: Queued requests respect request timeouts to prevent indefinite blocking

### Configuration Options

Datastore throttling can be configured for each API operation:

#### Check API Datastore Throttling

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `OPENFGA_CHECK_DATASTORE_THROTTLE_ENABLED` | `--check-datastore-throttle-enabled` | `false` | Enable datastore throttling for Check requests |
| `OPENFGA_CHECK_DATASTORE_THROTTLE_THRESHOLD` | `--check-datastore-throttle-threshold` | - | Maximum concurrent database operations |
| `OPENFGA_CHECK_DATASTORE_THROTTLE_DURATION` | `--check-datastore-throttle-duration` | - | How long to wait for database resources |

#### ListObjects API Datastore Throttling

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `OPENFGA_LIST_OBJECTS_DATASTORE_THROTTLE_ENABLED` | `--listObjects-datastore-throttle-enabled` | `false` | Enable datastore throttling for ListObjects requests |
| `OPENFGA_LIST_OBJECTS_DATASTORE_THROTTLE_THRESHOLD` | `--listObjects-datastore-throttle-threshold` | - | Maximum concurrent database operations |
| `OPENFGA_LIST_OBJECTS_DATASTORE_THROTTLE_DURATION` | `--listObjects-datastore-throttle-duration` | - | How long to wait for database resources |

#### ListUsers API Datastore Throttling

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `OPENFGA_LIST_USERS_DATASTORE_THROTTLE_ENABLED` | `--listUsers-datastore-throttle-enabled` | `false` | Enable datastore throttling for ListUsers requests |
| `OPENFGA_LIST_USERS_DATASTORE_THROTTLE_THRESHOLD` | `--listUsers-datastore-throttle-threshold` | - | Maximum concurrent database operations |
| `OPENFGA_LIST_USERS_DATASTORE_THROTTLE_DURATION` | `--listUsers-datastore-throttle-duration` | - | How long to wait for database resources |

## Configuration Examples

Enable dispatch throttling for all operations:

```bash
# Enable throttling for all APIs
export OPENFGA_CHECK_DISPATCH_THROTTLING_ENABLED=true
export OPENFGA_LIST_OBJECTS_DISPATCH_THROTTLING_ENABLED=true 
export OPENFGA_LIST_USERS_DISPATCH_THROTTLING_ENABLED=true

# Set dispatch thresholds
export OPENFGA_CHECK_DISPATCH_THROTTLING_THRESHOLD=3000
export OPENFGA_LIST_OBJECTS_DISPATCH_THROTTLING_THRESHOLD=3000
export OPENFGA_LIST_USERS_DISPATCH_THROTTLING_THRESHOLD=3000

# Configure processing frequency (how often throttled requests are processed)
export OPENFGA_CHECK_DISPATCH_THROTTLING_FREQUENCY=50ms
export OPENFGA_LIST_OBJECTS_DISPATCH_THROTTLING_FREQUENCY=50ms
export OPENFGA_LIST_USERS_DISPATCH_THROTTLING_FREQUENCY=50ms
```

### Datastore Throttling Setup

Configure datastore throttling to limit concurrent database operations:

```bash
# Enable datastore throttling
export OPENFGA_CHECK_DATASTORE_THROTTLE_ENABLED=true
export OPENFGA_LIST_OBJECTS_DATASTORE_THROTTLE_ENABLED=true
export OPENFGA_LIST_USERS_DATASTORE_THROTTLE_ENABLED=true

# Set concurrent operation limits (adjust based on your database capacity)
export OPENFGA_CHECK_DATASTORE_THROTTLE_THRESHOLD=20
export OPENFGA_LIST_OBJECTS_DATASTORE_THROTTLE_THRESHOLD=10
export OPENFGA_LIST_USERS_DATASTORE_THROTTLE_THRESHOLD=10

# Set wait timeouts
export OPENFGA_CHECK_DATASTORE_THROTTLE_DURATION=1s
export OPENFGA_LIST_OBJECTS_DATASTORE_THROTTLE_DURATION=2s
export OPENFGA_LIST_USERS_DATASTORE_THROTTLE_DURATION=2s
```

## Monitoring and Observability

OpenFGA provides several metrics to monitor throttling behavior:

### Dispatch Throttling Metrics

- `openfga_throttling_delay_ms`: Time spent waiting for dispatch throttling (histogram)
 - Labels: `grpc_service`, `grpc_method`, `throttler_name`
- `openfga_throttled_requests_count`: Count of throttled requests per operation

### Request Logging

Throttled requests are logged with additional context:

```json
{
 "level": "info",
 "msg": "Check request completed",
 "request_id": "...",
 "store_id": "...",
 "dispatch_count": 45,
 "request.throttled": true
}
```

## Troubleshooting

### Common Issues

**Requests Timing Out After Enabling Throttling**
- Throttling frequency too high
- Thresholds too low for your workload
- Datastore throttle duration exceeding request timeout

**No Throttling Effect Observed**
- Thresholds set too high for your workload
- Throttling not enabled
- Workload doesn't exceed thresholds

**Database Still Overloaded**
- Datastore throttle thresholds too high
- Throttling not enabled for all relevant operations
- Non-OpenFGA database load

### Debugging Steps

1. **Check Configuration**: Verify environment variables are set correctly
2. **Review Logs**: Look for `request.throttled` fields in request logs
3. **Monitor Metrics**: Watch `openfga_throttling_delay_ms` and `openfga_throttled_requests_count`
4. **Analyze Dispatch Counts**: Check typical dispatch counts in your workload
5. **Database Monitoring**: Monitor database connection usage and query performance

## Implementation Details

### Throttling Architecture

OpenFGA implements throttling through resolver decorators that wrap the core check/list logic:

- `DispatchThrottlingCheckResolver`: Handles dispatch throttling for Check operations
- Similar resolvers exist for ListObjects and ListUsers operations
- Constant-rate throttlers use Go channels and tickers for controlled processing
