/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.dynamic.config;

import io.atomix.cluster.MemberId;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Captures the zone placement of cluster members, used by partition distributors to ensure replicas
 * span availability zones.
 */
public final class TopologyConstraints {

  private final Map<MemberId, String> memberZones;

  public TopologyConstraints(final Map<MemberId, String> memberZones) {
    this.memberZones = Map.copyOf(memberZones);
  }

  public static TopologyConstraints unconstrained() {
    return new TopologyConstraints(Map.of());
  }

  public boolean isTopologyAware() {
    return !memberZones.isEmpty();
  }

  public Set<String> getZones() {
    return Set.copyOf(memberZones.values());
  }

  public Optional<String> getZoneForMember(final MemberId memberId) {
    return Optional.ofNullable(memberZones.get(memberId));
  }

  public Set<MemberId> getMembersInZone(final String zone) {
    return memberZones.entrySet().stream()
        .filter(e -> e.getValue().equals(zone))
        .map(Map.Entry::getKey)
        .collect(Collectors.toUnmodifiableSet());
  }

  public int zoneCount() {
    return (int) memberZones.values().stream().distinct().count();
  }
}
