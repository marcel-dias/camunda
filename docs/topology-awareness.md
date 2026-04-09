# Topology-Aware Cluster Configuration

## Overview

Zeebe supports topology-aware partition distribution, leader election, and gateway routing. When configured, partitions are distributed across availability zones to maximize fault tolerance, leaders are balanced across zones, and gateway routing prefers brokers in the same zone to reduce cross-zone latency.

All topology features are **opt-in**. When no zone/region is configured, Zeebe behaves exactly as before (round-robin distribution).

## Configuration

### Broker Configuration

Each broker can be configured with its zone and region:

```yaml
camunda:
  cluster:
    topology:
      zone: us-east-1a
      region: us-east-1
```

Or via environment variables:
```bash
CAMUNDA_CLUSTER_TOPOLOGY_ZONE=us-east-1a
CAMUNDA_CLUSTER_TOPOLOGY_REGION=us-east-1
```

### Gateway Configuration

The gateway uses the same configuration to enable zone-affinity routing:

```yaml
camunda:
  cluster:
    topology:
      zone: us-east-1a
```

When a zone is configured, the gateway prefers to route requests to partitions whose leader is in the same zone, reducing cross-zone network latency. If no local-zone leader is available, it falls back to standard round-robin routing.

## How It Works

### Partition Distribution

When topology is configured for **all** cluster members, the `TopologyAwarePartitionDistributor` ensures:
- Each partition's replicas span multiple zones (when possible)
- Replicas are distributed as evenly as possible across zones
- If the replication factor exceeds the zone count, extra replicas are spread evenly

If any member lacks topology configuration, the system falls back to round-robin distribution (all-or-nothing).

### Leader Election

The `ZoneAwarePriorityCalculator` assigns Raft election priorities so that leadership is distributed across zones. The preferred leader zone rotates by partition ID:
- Partition 1 prefers zone A
- Partition 2 prefers zone B  
- Partition 3 prefers zone C
- And so on...

This ensures no single zone becomes a hot spot for leadership.

### Gateway Zone-Affinity Routing

When the gateway's zone is configured, the `ZoneAffinityDispatchStrategy`:
1. Identifies partitions whose current leader is in the gateway's zone
2. Round-robins among those local-zone partitions
3. Falls back to global round-robin if no local leaders exist

This is a **soft preference** — availability is never sacrificed for locality.

## Kubernetes Deployment

### Using Topology Labels

Kubernetes provides topology labels that can be injected into pods:

```yaml
# In Zeebe broker StatefulSet
spec:
  template:
    spec:
      containers:
        - name: zeebe
          env:
            - name: CAMUNDA_CLUSTER_TOPOLOGY_ZONE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.labels['topology.kubernetes.io/zone']
            - name: CAMUNDA_CLUSTER_TOPOLOGY_REGION
              valueFrom:
                fieldRef:
                  fieldPath: metadata.labels['topology.kubernetes.io/region']
```

### Pod Topology Spread Constraints

Combine Zeebe's topology awareness with Kubernetes pod topology spread constraints:

```yaml
spec:
  template:
    spec:
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: topology.kubernetes.io/zone
          whenUnsatisfiable: DoNotSchedule
          labelSelector:
            matchLabels:
              app: camunda-zeebe
```

This ensures that Zeebe brokers are distributed across zones at the Kubernetes level, complementing Zeebe's internal topology-aware partition distribution.

## Observability

### Metrics

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `zeebe.cluster.topology.configured` | Gauge | - | 1.0 if topology-aware, 0.0 otherwise |
| `zeebe.cluster.topology.zone.members` | Gauge | `zone` | Number of members per zone |

### Startup Validation

On startup, the broker logs topology distribution information:
- `INFO` — Zone count and member distribution summary
- `WARN` — If RF > 1 but all brokers are in one zone (no zone fault tolerance)
- `WARN` — If RF > zone count (some zones will host multiple replicas)

Example log output:
```
INFO  [TopologyAwarePartitionDistributor] Topology-aware distribution enabled: 3 zones, 9 brokers
INFO  [TopologyAwarePartitionDistributor] Zone distribution: us-east-1a=3, us-east-1b=3, us-east-1c=3
WARN  [TopologyAwarePartitionDistributor] Replication factor (5) exceeds zone count (3) - some zones will host multiple replicas
```

## Best Practices

### Zone Planning

- **Replication Factor**: Set RF ≥ zone count for optimal fault tolerance
- **Broker Count**: Distribute brokers evenly across zones (e.g., 3 zones × 3 brokers = 9 total)
- **Network Latency**: Ensure cross-zone latency is acceptable for Raft consensus (typically < 10ms)

### Configuration Consistency

- Configure **all** brokers and gateways with topology information
- Use consistent zone naming across all components
- Validate configuration during deployment to avoid runtime fallbacks

### Monitoring

- Monitor the `zeebe.cluster.topology.configured` metric to ensure topology awareness is active
- Track zone-level broker availability to detect zone-wide failures
- Observe partition leader distribution across zones

## Limitations

- **Bootstrap**: At initial cluster formation, each broker only knows its own zone. Topology-aware distribution applies during dynamic reconfiguration when full cluster topology is available via gossip.
- **All-or-nothing**: If any active member lacks zone configuration, the system falls back to standard distribution. Ensure all brokers are configured consistently.
- **Cross-region latency**: For multi-region deployments, consider increasing Raft election timeouts to avoid unnecessary elections due to network latency.
- **Dynamic reconfiguration**: Adding brokers to a running cluster with topology configuration will trigger partition rebalancing to maintain optimal zone distribution.

## Fault Tolerance

In a 3-zone cluster with replication factor 3:
- Losing one entire zone still maintains quorum (2/3 replicas available)
- Leadership automatically fails over to a broker in a surviving zone
- The gateway automatically routes to surviving leaders

This is the primary resilience benefit of topology-aware distribution.

### Example Failure Scenarios

**Single Zone Failure (3-zone cluster, RF=3):**
- Before: Each partition has one replica per zone
- After zone failure: Each partition has 2/3 replicas remaining (quorum maintained)
- Recovery: Leaders automatically elected in surviving zones

**Single Broker Failure:**
- Topology-aware distribution minimizes impact by ensuring replicas are spread across zones
- Leadership election considers zone balance when selecting new leaders

## Migration from Round-Robin Distribution

To migrate an existing cluster to topology-aware distribution:

1. **Plan the migration**: Identify current partition distribution and target zone layout
2. **Configure brokers**: Add topology configuration to all brokers (requires restart)
3. **Verify configuration**: Check logs for topology detection and distribution warnings
4. **Monitor rebalancing**: Partition redistribution occurs automatically during the next configuration change

During migration, expect temporary partition rebalancing as the system optimizes for zone distribution.

## Troubleshooting

### Common Issues

**Fallback to round-robin distribution:**
- Verify all brokers have topology configuration
- Check for typos in zone names
- Ensure environment variables are properly set

**Uneven zone distribution:**
- Confirm broker count is divisible by zone count
- Check for failed brokers that may affect distribution calculations

**High cross-zone latency:**
- Consider increasing Raft election timeouts
- Verify network connectivity between zones
- Monitor gateway routing patterns

### Diagnostic Commands

Check current cluster topology:
```bash
# View broker cluster membership (includes topology info if configured)
zbctl status --address <broker-address>
```

Review broker logs for topology information:
```bash
# Look for topology-related log entries
grep -i topology /path/to/zeebe/logs/zeebe.log
```