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
import java.util.*;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TopologyAwarePartitionDistributorTest {

  private static final String GROUP = "raft-partition";

  @Nested
  class ThreeZoneCluster {

    // 6 nodes across 3 zones (2 per zone), RF=3, 3 partitions
    final Set<MemberId> members =
        Set.of(
            MemberId.from("0"), MemberId.from("1"),
            MemberId.from("2"), MemberId.from("3"),
            MemberId.from("4"), MemberId.from("5"));
    final TopologyConstraints constraints =
        new TopologyConstraints(
            Map.of(
                MemberId.from("0"),
                "zone-a",
                MemberId.from("1"),
                "zone-a",
                MemberId.from("2"),
                "zone-b",
                MemberId.from("3"),
                "zone-b",
                MemberId.from("4"),
                "zone-c",
                MemberId.from("5"),
                "zone-c"));

    @Test
    void shouldPlaceOneReplicaPerZone() {
      // given
      final var distributor = new TopologyAwarePartitionDistributor(constraints);
      final var partitions =
          List.of(
              PartitionId.from(GROUP, 1), PartitionId.from(GROUP, 2), PartitionId.from(GROUP, 3));

      // when
      final var result = distributor.distributePartitions(members, partitions, 3);

      // then — each partition has exactly 1 member per zone
      for (final var partition : result) {
        final var zones =
            partition.members().stream()
                .map(m -> constraints.getZoneForMember(m).orElseThrow())
                .collect(Collectors.toSet());
        assertThat(zones).containsExactlyInAnyOrder("zone-a", "zone-b", "zone-c");
      }
    }

    @Test
    void shouldDistributeLeadershipAcrossZones() {
      // given
      final var distributor = new TopologyAwarePartitionDistributor(constraints);
      final var partitions =
          List.of(
              PartitionId.from(GROUP, 1), PartitionId.from(GROUP, 2), PartitionId.from(GROUP, 3));

      // when
      final var result = distributor.distributePartitions(members, partitions, 3);

      // then — primary (highest priority) members should span all zones
      final var primaryZones =
          result.stream()
              .map(
                  p -> {
                    final var primary = p.getPrimary().orElseThrow();
                    return constraints.getZoneForMember(primary).orElseThrow();
                  })
              .collect(Collectors.toList());
      assertThat(primaryZones).containsExactlyInAnyOrder("zone-a", "zone-b", "zone-c");
    }
  }

  @Nested
  class TwoZoneCluster {

    final Set<MemberId> members =
        Set.of(
            MemberId.from("0"),
            MemberId.from("1"),
            MemberId.from("2"),
            MemberId.from("3"),
            MemberId.from("4"),
            MemberId.from("5"));
    final TopologyConstraints constraints =
        new TopologyConstraints(
            Map.of(
                MemberId.from("0"),
                "zone-a",
                MemberId.from("1"),
                "zone-a",
                MemberId.from("2"),
                "zone-a",
                MemberId.from("3"),
                "zone-b",
                MemberId.from("4"),
                "zone-b",
                MemberId.from("5"),
                "zone-b"));

    @Test
    void shouldSpreadReplicasAcrossZonesWhenRFExceedsZoneCount() {
      // given
      final var distributor = new TopologyAwarePartitionDistributor(constraints);
      final var partitions = List.of(PartitionId.from(GROUP, 1));

      // when
      final var result = distributor.distributePartitions(members, partitions, 3);

      // then — both zones must have at least 1 replica
      final var partition = result.iterator().next();
      final var zoneCounts =
          partition.members().stream()
              .collect(
                  Collectors.groupingBy(
                      (MemberId m) -> constraints.getZoneForMember(m).orElseThrow(),
                      Collectors.counting()));
      assertThat(zoneCounts).containsKeys("zone-a", "zone-b");
      // Difference between zones should be at most 1
      final var counts = zoneCounts.values();
      assertThat(
              Math.abs(
                  counts.stream().mapToLong(Long::longValue).max().orElse(0)
                      - counts.stream().mapToLong(Long::longValue).min().orElse(0)))
          .isLessThanOrEqualTo(1);
    }
  }

  @Nested
  class FallbackBehavior {

    @Test
    void shouldFallBackToRoundRobinWhenNoTopology() {
      // given
      final var distributor =
          new TopologyAwarePartitionDistributor(TopologyConstraints.unconstrained());
      final var members = Set.of(MemberId.from("0"), MemberId.from("1"), MemberId.from("2"));
      final var partitions =
          List.of(
              PartitionId.from(GROUP, 1), PartitionId.from(GROUP, 2), PartitionId.from(GROUP, 3));

      // when
      final var result = distributor.distributePartitions(members, partitions, 3);

      // then
      assertThat(result).hasSize(3);
      for (final var p : result) {
        assertThat(p.members()).hasSize(3);
      }
    }
  }

  @Nested
  class EdgeCases {

    @Test
    void shouldHandleSingleZoneCluster() {
      // given
      final Set<MemberId> members =
          Set.of(MemberId.from("0"), MemberId.from("1"), MemberId.from("2"));
      final TopologyConstraints constraints =
          new TopologyConstraints(
              Map.of(
                  MemberId.from("0"), "zone-a",
                  MemberId.from("1"), "zone-a",
                  MemberId.from("2"), "zone-a"));
      final var distributor = new TopologyAwarePartitionDistributor(constraints);
      final var partitions = List.of(PartitionId.from(GROUP, 1));

      // when
      final var result = distributor.distributePartitions(members, partitions, 3);

      // then
      final var partition = result.iterator().next();
      assertThat(partition.members()).hasSize(3);
    }

    @Test
    void shouldSatisfyReplicationFactorWithSmallZones() {
      // given — 2 zones with 1 member each, RF=2 (tight fit but possible)
      final Set<MemberId> members = Set.of(MemberId.from("0"), MemberId.from("1"));
      final TopologyConstraints constraints =
          new TopologyConstraints(
              Map.of(
                  MemberId.from("0"), "zone-a",
                  MemberId.from("1"), "zone-b"));
      final var distributor = new TopologyAwarePartitionDistributor(constraints);
      final var partitions = List.of(PartitionId.from(GROUP, 1));

      // when
      final var result = distributor.distributePartitions(members, partitions, 2);

      // then — RF=2 satisfied with 1 member per zone
      final var partition = result.iterator().next();
      assertThat(partition.members()).hasSize(2);
    }

    @Test
    void shouldUseAllAvailableMembersWhenRFExceedsTotalMembers() {
      // given — 2 members, RF=3 (impossible to fully satisfy)
      final Set<MemberId> members = Set.of(MemberId.from("0"), MemberId.from("1"));
      final TopologyConstraints constraints =
          new TopologyConstraints(
              Map.of(
                  MemberId.from("0"), "zone-a",
                  MemberId.from("1"), "zone-b"));
      final var distributor = new TopologyAwarePartitionDistributor(constraints);
      final var partitions = List.of(PartitionId.from(GROUP, 1));

      // when
      final var result = distributor.distributePartitions(members, partitions, 3);

      // then — uses all available members (can't exceed total member count)
      final var partition = result.iterator().next();
      assertThat(partition.members()).hasSize(2); // only 2 members available
    }

    @Test
    void shouldHandleAsymmetricZoneSizes() {
      // given — zone-a has 3 nodes, zone-b has 1 node, RF=3
      final Set<MemberId> members =
          Set.of(MemberId.from("0"), MemberId.from("1"), MemberId.from("2"), MemberId.from("3"));
      final TopologyConstraints constraints =
          new TopologyConstraints(
              Map.of(
                  MemberId.from("0"),
                  "zone-a",
                  MemberId.from("1"),
                  "zone-a",
                  MemberId.from("2"),
                  "zone-a",
                  MemberId.from("3"),
                  "zone-b"));
      final var distributor = new TopologyAwarePartitionDistributor(constraints);
      final var partitions = List.of(PartitionId.from(GROUP, 1));

      // when
      final var result = distributor.distributePartitions(members, partitions, 3);

      // then
      final var partition = result.iterator().next();
      assertThat(partition.members()).hasSize(3);
      final var zoneCounts =
          partition.members().stream()
              .collect(
                  Collectors.groupingBy(
                      (MemberId m) -> constraints.getZoneForMember(m).orElseThrow(),
                      Collectors.counting()));
      assertThat(zoneCounts).containsKeys("zone-a", "zone-b");
    }
  }
}
