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

  @Nested
  class TwoRegionCluster {

    // 8 brokers: 4 in region-us (2 zones), 4 in region-eu (2 zones)
    // Brokers 0-3 in region-us: 0,1 in us-east-1a, 2,3 in us-east-1b
    // Brokers 4-7 in region-eu: 4,5 in eu-west-1a, 6,7 in eu-west-1b
    final Set<MemberId> members =
        Set.of(
            MemberId.from("0"),
            MemberId.from("1"),
            MemberId.from("2"),
            MemberId.from("3"),
            MemberId.from("4"),
            MemberId.from("5"),
            MemberId.from("6"),
            MemberId.from("7"));

    final TopologyConstraints constraints =
        new TopologyConstraints(
            Map.of(
                MemberId.from("0"),
                "us-east-1a",
                MemberId.from("1"),
                "us-east-1a",
                MemberId.from("2"),
                "us-east-1b",
                MemberId.from("3"),
                "us-east-1b",
                MemberId.from("4"),
                "eu-west-1a",
                MemberId.from("5"),
                "eu-west-1a",
                MemberId.from("6"),
                "eu-west-1b",
                MemberId.from("7"),
                "eu-west-1b"),
            Map.of(
                MemberId.from("0"),
                "us-east",
                MemberId.from("1"),
                "us-east",
                MemberId.from("2"),
                "us-east",
                MemberId.from("3"),
                "us-east",
                MemberId.from("4"),
                "eu-west",
                MemberId.from("5"),
                "eu-west",
                MemberId.from("6"),
                "eu-west",
                MemberId.from("7"),
                "eu-west"));

    @Test
    void shouldDistributeReplicasEvenlyAcrossRegions() {
      // given — 8 partitions, RF=4
      final var distributor = new TopologyAwarePartitionDistributor(constraints);
      final var partitions =
          List.of(
              PartitionId.from(GROUP, 1),
              PartitionId.from(GROUP, 2),
              PartitionId.from(GROUP, 3),
              PartitionId.from(GROUP, 4),
              PartitionId.from(GROUP, 5),
              PartitionId.from(GROUP, 6),
              PartitionId.from(GROUP, 7),
              PartitionId.from(GROUP, 8));

      // when
      final var result = distributor.distributePartitions(members, partitions, 4);

      // then — each partition should have exactly 2 replicas per region
      for (final var partition : result) {
        final var regionCounts =
            partition.members().stream()
                .collect(
                    Collectors.groupingBy(
                        m -> constraints.getRegionForMember(m).orElseThrow(),
                        Collectors.counting()));
        assertThat(regionCounts.get("us-east")).isEqualTo(2);
        assertThat(regionCounts.get("eu-west")).isEqualTo(2);
      }
    }

    @Test
    void shouldBalanceLeadersAcrossRegions() {
      // given — 8 partitions, RF=4
      final var distributor = new TopologyAwarePartitionDistributor(constraints);
      final var partitions =
          List.of(
              PartitionId.from(GROUP, 1),
              PartitionId.from(GROUP, 2),
              PartitionId.from(GROUP, 3),
              PartitionId.from(GROUP, 4),
              PartitionId.from(GROUP, 5),
              PartitionId.from(GROUP, 6),
              PartitionId.from(GROUP, 7),
              PartitionId.from(GROUP, 8));

      // when
      final var result = distributor.distributePartitions(members, partitions, 4);

      // then — leaders should be split 4/4 across regions
      final var leaderRegions =
          result.stream()
              .map(
                  p -> {
                    final var primary = p.getPrimary().orElseThrow();
                    return constraints.getRegionForMember(primary).orElseThrow();
                  })
              .collect(Collectors.groupingBy(r -> r, Collectors.counting()));
      assertThat(leaderRegions.get("us-east")).isEqualTo(4);
      assertThat(leaderRegions.get("eu-west")).isEqualTo(4);
    }

    @Test
    void shouldEnsureDisasterRecoveryWithRegionLoss() {
      // given — 8 partitions, RF=4
      final var distributor = new TopologyAwarePartitionDistributor(constraints);
      final var partitions =
          List.of(
              PartitionId.from(GROUP, 1),
              PartitionId.from(GROUP, 2),
              PartitionId.from(GROUP, 3),
              PartitionId.from(GROUP, 4),
              PartitionId.from(GROUP, 5),
              PartitionId.from(GROUP, 6),
              PartitionId.from(GROUP, 7),
              PartitionId.from(GROUP, 8));

      // when
      final var result = distributor.distributePartitions(members, partitions, 4);

      // then — if us-east is lost, every partition still has 2 replicas in eu-west
      for (final var partition : result) {
        final long survivingReplicas =
            partition.members().stream()
                .filter(m -> "eu-west".equals(constraints.getRegionForMember(m).orElse(null)))
                .count();
        assertThat(survivingReplicas)
            .as("Partition %s should have 2 replicas surviving region loss", partition.id())
            .isEqualTo(2);
      }
    }
  }

  @Nested
  class ThreeRegionCluster {

    // 12 brokers: 4 per region, 2 zones per region
    // Region us-east: brokers 0-3 (zones us-east-1a, us-east-1b)
    // Region eu-west: brokers 4-7 (zones eu-west-1a, eu-west-1b)
    // Region ap-south: brokers 8-11 (zones ap-south-1a, ap-south-1b)
    final Set<MemberId> members =
        Set.of(
            MemberId.from("0"),
            MemberId.from("1"),
            MemberId.from("2"),
            MemberId.from("3"),
            MemberId.from("4"),
            MemberId.from("5"),
            MemberId.from("6"),
            MemberId.from("7"),
            MemberId.from("8"),
            MemberId.from("9"),
            MemberId.from("10"),
            MemberId.from("11"));

    final TopologyConstraints constraints =
        new TopologyConstraints(
            // zones
            Map.ofEntries(
                Map.entry(MemberId.from("0"), "us-east-1a"),
                Map.entry(MemberId.from("1"), "us-east-1a"),
                Map.entry(MemberId.from("2"), "us-east-1b"),
                Map.entry(MemberId.from("3"), "us-east-1b"),
                Map.entry(MemberId.from("4"), "eu-west-1a"),
                Map.entry(MemberId.from("5"), "eu-west-1a"),
                Map.entry(MemberId.from("6"), "eu-west-1b"),
                Map.entry(MemberId.from("7"), "eu-west-1b"),
                Map.entry(MemberId.from("8"), "ap-south-1a"),
                Map.entry(MemberId.from("9"), "ap-south-1a"),
                Map.entry(MemberId.from("10"), "ap-south-1b"),
                Map.entry(MemberId.from("11"), "ap-south-1b")),
            // regions
            Map.ofEntries(
                Map.entry(MemberId.from("0"), "us-east"),
                Map.entry(MemberId.from("1"), "us-east"),
                Map.entry(MemberId.from("2"), "us-east"),
                Map.entry(MemberId.from("3"), "us-east"),
                Map.entry(MemberId.from("4"), "eu-west"),
                Map.entry(MemberId.from("5"), "eu-west"),
                Map.entry(MemberId.from("6"), "eu-west"),
                Map.entry(MemberId.from("7"), "eu-west"),
                Map.entry(MemberId.from("8"), "ap-south"),
                Map.entry(MemberId.from("9"), "ap-south"),
                Map.entry(MemberId.from("10"), "ap-south"),
                Map.entry(MemberId.from("11"), "ap-south")));

    private List<PartitionId> eightPartitions() {
      return List.of(
          PartitionId.from(GROUP, 1),
          PartitionId.from(GROUP, 2),
          PartitionId.from(GROUP, 3),
          PartitionId.from(GROUP, 4),
          PartitionId.from(GROUP, 5),
          PartitionId.from(GROUP, 6),
          PartitionId.from(GROUP, 7),
          PartitionId.from(GROUP, 8));
    }

    @Test
    void shouldPlaceReplicasInAllThreeRegionsWithRF5() {
      // given — 8 partitions, RF=5 (5/3 = 1 base + 2 extra → 2+2+1 distribution)
      final var distributor = new TopologyAwarePartitionDistributor(constraints);

      // when
      final var result = distributor.distributePartitions(members, eightPartitions(), 5);

      // then — every partition has replicas in all 3 regions
      for (final var partition : result) {
        final var regionCounts =
            partition.members().stream()
                .collect(
                    Collectors.groupingBy(
                        m -> constraints.getRegionForMember(m).orElseThrow(),
                        Collectors.counting()));
        assertThat(regionCounts).containsKeys("us-east", "eu-west", "ap-south");
        assertThat(partition.members()).hasSize(5);
      }
    }

    @Test
    void shouldDistributeReplicasAcrossRegionsWithRF5() {
      // given — RF=5, 3 regions → each partition gets 2+2+1 replicas across regions
      final var distributor = new TopologyAwarePartitionDistributor(constraints);

      // when
      final var result = distributor.distributePartitions(members, eightPartitions(), 5);

      // then — each region has at least 1 and at most 2 replicas per partition
      for (final var partition : result) {
        final var regionCounts =
            partition.members().stream()
                .collect(
                    Collectors.groupingBy(
                        m -> constraints.getRegionForMember(m).orElseThrow(),
                        Collectors.counting()));
        for (final var count : regionCounts.values()) {
          assertThat(count)
              .as("Region replica count for partition %s", partition.id())
              .isBetween(1L, 2L);
        }
      }
    }

    @Test
    void shouldDistributeReplicasEvenlyAcrossRegionsWithRF6() {
      // given — RF=6, 3 regions → perfectly even: 2 replicas per region
      final var distributor = new TopologyAwarePartitionDistributor(constraints);

      // when
      final var result = distributor.distributePartitions(members, eightPartitions(), 6);

      // then — each partition has exactly 2 replicas per region
      for (final var partition : result) {
        final var regionCounts =
            partition.members().stream()
                .collect(
                    Collectors.groupingBy(
                        m -> constraints.getRegionForMember(m).orElseThrow(),
                        Collectors.counting()));
        assertThat(regionCounts.get("us-east")).isEqualTo(2);
        assertThat(regionCounts.get("eu-west")).isEqualTo(2);
        assertThat(regionCounts.get("ap-south")).isEqualTo(2);
      }
    }

    @Test
    void shouldBalanceLeadersAcrossThreeRegions() {
      // given — 8 partitions, RF=5, 3 regions → leaders rotate: ~3+3+2 or similar
      final var distributor = new TopologyAwarePartitionDistributor(constraints);

      // when
      final var result = distributor.distributePartitions(members, eightPartitions(), 5);

      // then — no region has more than ceil(8/3)=3 leaders, none has fewer than floor(8/3)=2
      final var leaderRegions =
          result.stream()
              .map(
                  p -> {
                    final var primary = p.getPrimary().orElseThrow();
                    return constraints.getRegionForMember(primary).orElseThrow();
                  })
              .collect(Collectors.groupingBy(r -> r, Collectors.counting()));
      assertThat(leaderRegions).hasSize(3);
      for (final var count : leaderRegions.values()) {
        assertThat(count)
            .as("Leader count per region should be balanced (2 or 3 for 8 partitions / 3 regions)")
            .isBetween(2L, 3L);
      }
    }

    @Test
    void shouldSurviveAnyOneRegionLossWithRF5() {
      // given — RF=5, quorum=3 → losing any region (max 2 replicas) leaves ≥3
      final var distributor = new TopologyAwarePartitionDistributor(constraints);

      // when
      final var result = distributor.distributePartitions(members, eightPartitions(), 5);

      // then — for each region, simulating its loss leaves ≥3 replicas per partition
      for (final var lostRegion : List.of("us-east", "eu-west", "ap-south")) {
        for (final var partition : result) {
          final long survivingReplicas =
              partition.members().stream()
                  .filter(m -> !lostRegion.equals(constraints.getRegionForMember(m).orElse(null)))
                  .count();
          assertThat(survivingReplicas)
              .as(
                  "Partition %s should have ≥3 replicas after losing %s",
                  partition.id(), lostRegion)
              .isGreaterThanOrEqualTo(3);
        }
      }
    }

    @Test
    void shouldSurviveAnyOneRegionLossWithRF6() {
      // given — RF=6, quorum=4 → losing any region (exactly 2 replicas) leaves 4
      final var distributor = new TopologyAwarePartitionDistributor(constraints);

      // when
      final var result = distributor.distributePartitions(members, eightPartitions(), 6);

      // then — losing any region leaves exactly 4 replicas (quorum maintained)
      for (final var lostRegion : List.of("us-east", "eu-west", "ap-south")) {
        for (final var partition : result) {
          final long survivingReplicas =
              partition.members().stream()
                  .filter(m -> !lostRegion.equals(constraints.getRegionForMember(m).orElse(null)))
                  .count();
          assertThat(survivingReplicas)
              .as("Partition %s should have 4 replicas after losing %s", partition.id(), lostRegion)
              .isEqualTo(4);
        }
      }
    }
  }
}
