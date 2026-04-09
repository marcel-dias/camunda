/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.broker.client.impl;

import io.camunda.zeebe.broker.client.api.BrokerClusterState;
import io.camunda.zeebe.broker.client.api.BrokerTopologyManager;
import io.camunda.zeebe.broker.client.api.RequestDispatchStrategy;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dispatch strategy that prefers partitions whose leader is in the same zone as this gateway. Falls
 * back to round-robin across all partitions when no local-zone leader exists. Thread-safe.
 */
public final class ZoneAffinityDispatchStrategy implements RequestDispatchStrategy {

  private final String localZone;
  private final AtomicLong offset;
  private final RoundRobinDispatchStrategy fallback;

  public ZoneAffinityDispatchStrategy(final String localZone) {
    this(localZone, 0);
  }

  public ZoneAffinityDispatchStrategy(final String localZone, final int initialOffset) {
    this.localZone = localZone;
    this.offset = new AtomicLong(initialOffset);
    this.fallback = new RoundRobinDispatchStrategy(initialOffset);
  }

  @Override
  public int determinePartition(final BrokerTopologyManager topologyManager) {
    final BrokerClusterState topology = topologyManager.getTopology();
    if (topology == null || !topology.isInitialized()) {
      return BrokerClusterState.PARTITION_ID_NULL;
    }

    // Collect partitions with local-zone leaders
    final var partitions = topology.getPartitions();
    final var localPartitions = new ArrayList<Integer>();
    for (final var partitionId : partitions) {
      final int leaderId = topology.getLeaderForPartition(partitionId);
      if (leaderId != BrokerClusterState.NODE_ID_NULL) {
        final String leaderZone = topology.getBrokerZone(leaderId);
        if (localZone.equals(leaderZone)) {
          localPartitions.add(partitionId);
        }
      }
    }

    if (localPartitions.isEmpty()) {
      // Fall back to round-robin across all partitions
      return fallback.determinePartition(topologyManager);
    }

    // Round-robin among local-zone partitions
    final int index = Math.floorMod(offset.getAndIncrement(), localPartitions.size());
    return localPartitions.get(index);
  }

  public String getLocalZone() {
    return localZone;
  }
}
