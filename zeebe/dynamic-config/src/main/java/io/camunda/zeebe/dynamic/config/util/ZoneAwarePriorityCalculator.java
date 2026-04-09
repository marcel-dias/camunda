/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.dynamic.config.util;

import io.atomix.cluster.MemberId;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Computes topology-aware priorities for Raft partition members. The preferred leader zone rotates
 * based on partition ID to distribute leadership evenly across zones.
 */
public final class ZoneAwarePriorityCalculator {

  private ZoneAwarePriorityCalculator() {}

  /**
   * Compute priorities such that the preferred leader zone rotates by partition ID.
   *
   * @param partitionId the partition number (1-based)
   * @param memberZones map of member → zone for each member in this partition
   * @param replicationFactor the replication factor
   * @return map of member → priority (higher = more preferred for leadership)
   */
  public static Map<MemberId, Integer> computePriorities(
      final int partitionId, final Map<MemberId, String> memberZones, final int replicationFactor) {

    final var sortedZones = memberZones.values().stream().distinct().sorted().toList();
    final int zoneCount = sortedZones.size();

    // Group members by zone, sorted deterministically within each zone
    final var membersByZone = new LinkedHashMap<String, List<MemberId>>();
    for (final var zone : sortedZones) {
      membersByZone.put(
          zone,
          memberZones.entrySet().stream()
              .filter(e -> e.getValue().equals(zone))
              .map(Map.Entry::getKey)
              .sorted(Comparator.comparing(MemberId::id))
              .toList());
    }

    // Rotate starting zone based on partition ID
    final int startZoneIdx = (partitionId - 1) % zoneCount;

    final var priorities = new HashMap<MemberId, Integer>();
    int currentPriority = replicationFactor;

    for (int i = 0; i < zoneCount; i++) {
      final int zIdx = (startZoneIdx + i) % zoneCount;
      final var zone = sortedZones.get(zIdx);
      for (final var member : membersByZone.get(zone)) {
        if (priorities.size() < replicationFactor) {
          priorities.put(member, currentPriority--);
        }
      }
    }
    return Map.copyOf(priorities);
  }

  /**
   * Compute priorities with region-level rotation. The preferred leader region rotates by partition
   * ID. Within the preferred region, members get the highest priorities.
   *
   * @param partitionId the partition number (1-based)
   * @param memberZones map of member → zone for each member in this partition
   * @param memberRegions map of member → region for each member in this partition
   * @param replicationFactor the replication factor
   * @return map of member → priority (higher = more preferred for leadership)
   */
  public static Map<MemberId, Integer> computePriorities(
      final int partitionId,
      final Map<MemberId, String> memberZones,
      final Map<MemberId, String> memberRegions,
      final int replicationFactor) {

    if (memberRegions.isEmpty()) {
      return computePriorities(partitionId, memberZones, replicationFactor);
    }

    final var sortedRegions = memberRegions.values().stream().distinct().sorted().toList();
    final int regionCount = sortedRegions.size();

    // Group members by region, sorted within each region
    final var membersByRegion = new LinkedHashMap<String, List<MemberId>>();
    for (final var region : sortedRegions) {
      membersByRegion.put(
          region,
          memberRegions.entrySet().stream()
              .filter(e -> e.getValue().equals(region))
              .map(Map.Entry::getKey)
              .sorted(Comparator.comparing(MemberId::id))
              .toList());
    }

    // Rotate starting region by partition ID
    final int startRegionIdx = (partitionId - 1) % regionCount;

    final var priorities = new HashMap<MemberId, Integer>();
    int currentPriority = replicationFactor;

    for (int i = 0; i < regionCount; i++) {
      final int rIdx = (startRegionIdx + i) % regionCount;
      final var region = sortedRegions.get(rIdx);
      for (final var member : membersByRegion.get(region)) {
        if (priorities.size() < replicationFactor) {
          priorities.put(member, currentPriority--);
        }
      }
    }
    return Map.copyOf(priorities);
  }
}
