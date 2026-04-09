/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.dynamic.config.util;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionMetadata;
import io.camunda.zeebe.dynamic.config.PartitionDistributor;
import io.camunda.zeebe.dynamic.config.TopologyConstraints;
import java.util.*;
import java.util.stream.Collectors;

public class TopologyAwarePartitionDistributor implements PartitionDistributor {

  private final TopologyConstraints constraints;
  private final RoundRobinPartitionDistributor fallback = new RoundRobinPartitionDistributor();

  public TopologyAwarePartitionDistributor(final TopologyConstraints constraints) {
    this.constraints = constraints;
  }

  @Override
  public Set<PartitionMetadata> distributePartitions(
      final Set<MemberId> clusterMembers,
      final List<PartitionId> sortedPartitionIds,
      final int replicationFactor) {

    if (!constraints.isTopologyAware()) {
      return fallback.distributePartitions(clusterMembers, sortedPartitionIds, replicationFactor);
    }

    final var sortedZones = constraints.getZones().stream().sorted().toList();
    final var membersByZone = new LinkedHashMap<String, List<MemberId>>();
    for (final var zone : sortedZones) {
      membersByZone.put(
          zone,
          constraints.getMembersInZone(zone).stream()
              .filter(clusterMembers::contains)
              .sorted(Comparator.comparing(MemberId::id))
              .collect(Collectors.toCollection(ArrayList::new)));
    }

    final int zoneCount = sortedZones.size();
    final var result = new LinkedHashSet<PartitionMetadata>();

    // Track round-robin index per zone
    final var zoneIndexes = new HashMap<String, Integer>();
    sortedZones.forEach(z -> zoneIndexes.put(z, 0));

    for (int pIdx = 0; pIdx < sortedPartitionIds.size(); pIdx++) {
      final var partitionId = sortedPartitionIds.get(pIdx);
      final var selectedMembers = new LinkedHashSet<MemberId>();

      // Distribute replicas across zones as evenly as possible
      final int replicasPerZone = replicationFactor / zoneCount;
      final int extraReplicas = replicationFactor % zoneCount;

      // Rotate starting zone per partition for leadership distribution
      final int startZoneIdx = pIdx % zoneCount;

      for (int i = 0; i < zoneCount && selectedMembers.size() < replicationFactor; i++) {
        final int zIdx = (startZoneIdx + i) % zoneCount;
        final var zone = sortedZones.get(zIdx);
        final var zoneMembers = membersByZone.get(zone);
        if (zoneMembers.isEmpty()) {
          continue;
        }

        final int count = replicasPerZone + (i < extraReplicas ? 1 : 0);
        for (int r = 0; r < count && selectedMembers.size() < replicationFactor; r++) {
          final int idx = zoneIndexes.get(zone);
          selectedMembers.add(zoneMembers.get(idx % zoneMembers.size()));
          zoneIndexes.put(zone, idx + 1);
        }
      }

      // Second pass: fill remaining slots from zones with spare members
      if (selectedMembers.size() < replicationFactor) {
        for (int i = 0; i < zoneCount && selectedMembers.size() < replicationFactor; i++) {
          final int zIdx = (startZoneIdx + i) % zoneCount;
          final var zone = sortedZones.get(zIdx);
          final var zoneMembers = membersByZone.get(zone);
          for (final var member : zoneMembers) {
            if (selectedMembers.size() >= replicationFactor) {
              break;
            }
            selectedMembers.add(member); // LinkedHashSet ignores duplicates
          }
        }
      }

      // Assign priorities — first member (from the starting zone) gets highest
      final var members = new ArrayList<>(selectedMembers);
      final var priorities = new HashMap<MemberId, Integer>();
      for (int i = 0; i < members.size(); i++) {
        priorities.put(members.get(i), replicationFactor - i);
      }
      final int targetPriority = replicationFactor;
      final var primary = members.get(0);

      result.add(
          new PartitionMetadata(partitionId, selectedMembers, priorities, targetPriority, primary));
    }

    return result;
  }
}
