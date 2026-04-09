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
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ZoneAwarePriorityCalculatorTest {

  @Test
  void shouldAssignHighestPriorityToPreferredZone() {
    // given — 3 members in 3 zones
    final var memberZones =
        Map.of(
            MemberId.from("0"), "zone-a",
            MemberId.from("1"), "zone-b",
            MemberId.from("2"), "zone-c");

    // when — partition 1 (index 0 → zone-a gets highest)
    final var priorities = ZoneAwarePriorityCalculator.computePriorities(1, memberZones, 3);

    // then — zone-a member gets highest priority
    final var maxEntry =
        priorities.entrySet().stream().max(Map.Entry.comparingByValue()).orElseThrow();
    assertThat(memberZones.get(maxEntry.getKey())).isEqualTo("zone-a");
  }

  @Test
  void shouldRotatePreferredZoneByPartitionId() {
    // given
    final var memberZones =
        Map.of(
            MemberId.from("0"), "zone-a",
            MemberId.from("1"), "zone-b",
            MemberId.from("2"), "zone-c");

    // when — compute for partitions 1, 2, 3
    final var p1 = ZoneAwarePriorityCalculator.computePriorities(1, memberZones, 3);
    final var p2 = ZoneAwarePriorityCalculator.computePriorities(2, memberZones, 3);
    final var p3 = ZoneAwarePriorityCalculator.computePriorities(3, memberZones, 3);

    // then — each partition has a different highest-priority zone
    final var leader1Zone = getHighestPriorityZone(p1, memberZones);
    final var leader2Zone = getHighestPriorityZone(p2, memberZones);
    final var leader3Zone = getHighestPriorityZone(p3, memberZones);
    assertThat(Set.of(leader1Zone, leader2Zone, leader3Zone)).hasSize(3);
  }

  @Test
  void shouldHandleSingleZone() {
    // given — all members in same zone
    final var memberZones =
        Map.of(
            MemberId.from("0"), "zone-a",
            MemberId.from("1"), "zone-a");

    // when
    final var priorities = ZoneAwarePriorityCalculator.computePriorities(1, memberZones, 2);

    // then — should still produce valid priorities
    assertThat(priorities).hasSize(2);
    assertThat(priorities.values()).allMatch(p -> p > 0);
  }

  @Test
  void shouldAssignDescendingPriorities() {
    // given
    final var memberZones =
        Map.of(
            MemberId.from("0"), "zone-a",
            MemberId.from("1"), "zone-b",
            MemberId.from("2"), "zone-c");

    // when
    final var priorities = ZoneAwarePriorityCalculator.computePriorities(1, memberZones, 3);

    // then — priorities should be 3, 2, 1
    assertThat(priorities.values()).containsExactlyInAnyOrder(3, 2, 1);
  }

  @Test
  void shouldProduceDeterministicResults() {
    // given
    final var memberZones =
        Map.of(
            MemberId.from("0"), "zone-a",
            MemberId.from("1"), "zone-b",
            MemberId.from("2"), "zone-c");

    // when — call twice with same inputs
    final var first = ZoneAwarePriorityCalculator.computePriorities(1, memberZones, 3);
    final var second = ZoneAwarePriorityCalculator.computePriorities(1, memberZones, 3);

    // then — results must be identical
    assertThat(first).isEqualTo(second);
  }

  private String getHighestPriorityZone(
      final Map<MemberId, Integer> priorities, final Map<MemberId, String> zones) {
    return priorities.entrySet().stream()
        .max(Map.Entry.comparingByValue())
        .map(e -> zones.get(e.getKey()))
        .orElseThrow();
  }
}
