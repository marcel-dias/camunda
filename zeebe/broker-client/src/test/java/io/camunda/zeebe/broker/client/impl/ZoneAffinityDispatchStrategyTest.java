/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.broker.client.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.broker.client.api.BrokerClusterState;
import org.junit.jupiter.api.Test;

final class ZoneAffinityDispatchStrategyTest {

  @Test
  void shouldPreferPartitionWithLocalZoneLeader() {
    // given — gateway in zone-a; partition 1 leader (broker 0) in zone-a,
    //         partition 2 leader (broker 1) in zone-b
    final var strategy = new ZoneAffinityDispatchStrategy("zone-a", 0);
    final var topologyManager = new TestTopologyManager();
    topologyManager
        .addPartition(1, 0)
        .addPartition(2, 1)
        .setBrokerZone(0, "zone-a")
        .setBrokerZone(1, "zone-b");

    // when
    final var partition = strategy.determinePartition(topologyManager);

    // then — should pick partition 1 (leader in zone-a)
    assertThat(partition).isEqualTo(1);
  }

  @Test
  void shouldRoundRobinAmongLocalZonePartitions() {
    // given — gateway in zone-a; partitions 1 and 3 have leaders in zone-a
    final var strategy = new ZoneAffinityDispatchStrategy("zone-a", 0);
    final var topologyManager = new TestTopologyManager();
    topologyManager
        .addPartition(1, 0)
        .addPartition(2, 1)
        .addPartition(3, 2)
        .setBrokerZone(0, "zone-a")
        .setBrokerZone(1, "zone-b")
        .setBrokerZone(2, "zone-a");

    // when — call twice
    final var first = strategy.determinePartition(topologyManager);
    final var second = strategy.determinePartition(topologyManager);

    // then — should alternate between partitions 1 and 3
    assertThat(first).isIn(1, 3);
    assertThat(second).isIn(1, 3);
    assertThat(first).isNotEqualTo(second);
  }

  @Test
  void shouldFallBackToRoundRobinWhenNoLocalLeader() {
    // given — gateway in zone-c; no leaders in zone-c
    final var strategy = new ZoneAffinityDispatchStrategy("zone-c", 0);
    final var topologyManager = new TestTopologyManager();
    topologyManager
        .addPartition(1, 0)
        .addPartition(2, 1)
        .setBrokerZone(0, "zone-a")
        .setBrokerZone(1, "zone-b");

    // when
    final var partition = strategy.determinePartition(topologyManager);

    // then — should return a valid partition (fallback round-robin)
    assertThat(partition).isIn(1, 2);
  }

  @Test
  void shouldReturnNullPartitionWhenNoTopology() {
    // given
    final var strategy = new ZoneAffinityDispatchStrategy("zone-a");
    final var topologyManager = new TestTopologyManager(null);

    // when
    final var partition = strategy.determinePartition(topologyManager);

    // then
    assertThat(partition).isEqualTo(BrokerClusterState.PARTITION_ID_NULL);
  }

  @Test
  void shouldFallBackWhenBrokerZoneIsNull() {
    // given — gateway in zone-a, but brokers have no zone configured
    final var strategy = new ZoneAffinityDispatchStrategy("zone-a", 0);
    final var topologyManager = new TestTopologyManager();
    topologyManager.addPartition(1, 0).addPartition(2, 1);
    // no setBrokerZone calls — zones are null

    // when
    final var partition = strategy.determinePartition(topologyManager);

    // then — should fallback to round-robin
    assertThat(partition).isIn(1, 2);
  }
}
