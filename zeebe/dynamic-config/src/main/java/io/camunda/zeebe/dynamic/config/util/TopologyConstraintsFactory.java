/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.dynamic.config.util;

import io.atomix.cluster.MemberId;
import io.camunda.zeebe.dynamic.config.TopologyConstraints;
import io.camunda.zeebe.dynamic.config.state.ClusterConfiguration;
import io.camunda.zeebe.dynamic.config.state.MemberState;
import java.util.HashMap;

/**
 * Builds TopologyConstraints from the ClusterConfiguration's MemberState topology. Returns
 * unconstrained if any ACTIVE/JOINING member lacks zone info (all-or-nothing).
 */
public final class TopologyConstraintsFactory {

  private TopologyConstraintsFactory() {}

  public static TopologyConstraints fromClusterConfiguration(final ClusterConfiguration config) {
    final var memberZones = new HashMap<MemberId, String>();

    for (final var entry : config.members().entrySet()) {
      final var memberId = entry.getKey();
      final var memberState = entry.getValue();

      // Only consider active or joining members
      if (memberState.state() != MemberState.State.ACTIVE
          && memberState.state() != MemberState.State.JOINING) {
        continue;
      }

      final var topology = memberState.topology();
      if (topology == null || !topology.isConfigured() || topology.getZone() == null) {
        // All-or-nothing: if any active member lacks topology, fall back
        return TopologyConstraints.unconstrained();
      }
      memberZones.put(memberId, topology.getZone());
    }

    if (memberZones.isEmpty()) {
      return TopologyConstraints.unconstrained();
    }
    return new TopologyConstraints(memberZones);
  }
}
