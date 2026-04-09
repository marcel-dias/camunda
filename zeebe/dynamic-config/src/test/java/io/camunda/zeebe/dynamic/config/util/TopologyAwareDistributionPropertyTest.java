/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.dynamic.config.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.partition.PartitionId;
import io.camunda.zeebe.dynamic.config.TopologyConstraints;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

class TopologyAwareDistributionPropertyTest {

  private static final String GROUP = "raft-partition";

  @Property(tries = 200)
  void shouldNeverPlaceAllReplicasInSameZoneWhenMultipleZonesExist(
      @ForAll @IntRange(min = 2, max = 6) final int zoneCount,
      @ForAll @IntRange(min = 2, max = 4) final int nodesPerZone,
      @ForAll @IntRange(min = 1, max = 12) final int partitionCount) {

    // given
    final int rf = Math.min(3, zoneCount * nodesPerZone);
    if (rf < 2) {
      return; // need at least RF=2 for meaningful zone spread
    }

    final var memberZones = new HashMap<MemberId, String>();
    final var allMembers = new HashSet<MemberId>();
    for (int z = 0; z < zoneCount; z++) {
      for (int n = 0; n < nodesPerZone; n++) {
        final var id = MemberId.from(String.valueOf(z * nodesPerZone + n));
        memberZones.put(id, "zone-" + z);
        allMembers.add(id);
      }
    }

    final var constraints = new TopologyConstraints(memberZones);
    final var distributor = new TopologyAwarePartitionDistributor(constraints);
    final var partitions =
        IntStream.rangeClosed(1, partitionCount).mapToObj(i -> PartitionId.from(GROUP, i)).toList();

    // when
    final var result = distributor.distributePartitions(allMembers, partitions, rf);

    // then — no partition should have all replicas in a single zone
    for (final var partition : result) {
      final var zones =
          partition.members().stream()
              .map(m -> constraints.getZoneForMember(m).orElseThrow())
              .collect(Collectors.toSet());
      assertThat(zones)
          .describedAs("Partition %s should span multiple zones", partition.id())
          .hasSizeGreaterThan(1);
    }
  }

  @Property(tries = 200)
  void shouldDistributeExactlyOneReplicaPerZoneWhenRFEqualsZoneCount(
      @ForAll @IntRange(min = 2, max = 4) final int zoneCount,
      @ForAll @IntRange(min = 1, max = 8) final int partitionCount) {

    // given — 2 nodes per zone, RF = zoneCount
    final int nodesPerZone = 2;
    final var memberZones = new HashMap<MemberId, String>();
    final var allMembers = new HashSet<MemberId>();
    for (int z = 0; z < zoneCount; z++) {
      for (int n = 0; n < nodesPerZone; n++) {
        final var id = MemberId.from(String.valueOf(z * nodesPerZone + n));
        memberZones.put(id, "zone-" + z);
        allMembers.add(id);
      }
    }

    final var constraints = new TopologyConstraints(memberZones);
    final var distributor = new TopologyAwarePartitionDistributor(constraints);
    final var partitions =
        IntStream.rangeClosed(1, partitionCount).mapToObj(i -> PartitionId.from(GROUP, i)).toList();

    // when
    final var result = distributor.distributePartitions(allMembers, partitions, zoneCount);

    // then — each partition has exactly 1 replica per zone
    for (final var partition : result) {
      final var zoneCounts =
          partition.members().stream()
              .collect(
                  Collectors.groupingBy(
                      m -> constraints.getZoneForMember(m).orElseThrow(), Collectors.counting()));
      for (final var count : zoneCounts.values()) {
        assertThat(count)
            .describedAs("Each zone should have exactly 1 replica for partition %s", partition.id())
            .isEqualTo(1L);
      }
    }
  }

  @Property(tries = 100)
  void shouldAlwaysReturnCorrectNumberOfPartitions(
      @ForAll @IntRange(min = 1, max = 5) final int zoneCount,
      @ForAll @IntRange(min = 1, max = 3) final int nodesPerZone,
      @ForAll @IntRange(min = 1, max = 12) final int partitionCount) {

    // given
    final int totalNodes = zoneCount * nodesPerZone;
    final int rf = Math.min(3, totalNodes);

    final var memberZones = new HashMap<MemberId, String>();
    final var allMembers = new HashSet<MemberId>();
    for (int z = 0; z < zoneCount; z++) {
      for (int n = 0; n < nodesPerZone; n++) {
        final var id = MemberId.from(String.valueOf(z * nodesPerZone + n));
        memberZones.put(id, "zone-" + z);
        allMembers.add(id);
      }
    }

    final var constraints = new TopologyConstraints(memberZones);
    final var distributor = new TopologyAwarePartitionDistributor(constraints);
    final var partitions =
        IntStream.rangeClosed(1, partitionCount).mapToObj(i -> PartitionId.from(GROUP, i)).toList();

    // when
    final var result = distributor.distributePartitions(allMembers, partitions, rf);

    // then
    assertThat(result).hasSize(partitionCount);
    for (final var partition : result) {
      assertThat(partition.members()).hasSize(rf);
    }
  }
}
