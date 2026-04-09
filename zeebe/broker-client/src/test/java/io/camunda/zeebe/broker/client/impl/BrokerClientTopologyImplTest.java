/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.broker.client.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.protocol.impl.encoding.BrokerInfo;
import java.util.List;
import org.junit.jupiter.api.Test;

class BrokerClientTopologyImplTest {

  @Test
  void shouldExposeBrokerZoneFromBrokerInfo() {
    // given
    final var brokerInfo =
        new BrokerInfo()
            .setNodeId(0)
            .setPartitionsCount(1)
            .setClusterSize(3)
            .setReplicationFactor(3)
            .setCommandApiAddress("localhost:26501")
            .setTopologyZone("us-east-1a");

    final var topology =
        BrokerClientTopologyImpl.fromMemberProperties(
            List.of(brokerInfo),
            new BrokerClientTopologyImpl.ConfiguredClusterState(
                3, 1, 3, List.of(1), -1, "test", 0));

    // when
    final var zone = topology.getBrokerZone(0);

    // then
    assertThat(zone).isEqualTo("us-east-1a");
  }

  @Test
  void shouldReturnNullZoneWhenNotConfigured() {
    // given
    final var brokerInfo =
        new BrokerInfo()
            .setNodeId(0)
            .setPartitionsCount(1)
            .setClusterSize(1)
            .setReplicationFactor(1)
            .setCommandApiAddress("localhost:26501");

    final var topology =
        BrokerClientTopologyImpl.fromMemberProperties(
            List.of(brokerInfo),
            new BrokerClientTopologyImpl.ConfiguredClusterState(
                1, 1, 1, List.of(1), -1, "test", 0));

    // when
    final var zone = topology.getBrokerZone(0);

    // then
    assertThat(zone).isNull();
  }

  @Test
  void shouldTrackZonesForMultipleBrokers() {
    // given
    final var broker0 =
        new BrokerInfo()
            .setNodeId(0)
            .setPartitionsCount(1)
            .setClusterSize(3)
            .setReplicationFactor(3)
            .setCommandApiAddress("localhost:26501")
            .setTopologyZone("zone-a");
    final var broker1 =
        new BrokerInfo()
            .setNodeId(1)
            .setPartitionsCount(1)
            .setClusterSize(3)
            .setReplicationFactor(3)
            .setCommandApiAddress("localhost:26502")
            .setTopologyZone("zone-b");
    final var broker2 =
        new BrokerInfo()
            .setNodeId(2)
            .setPartitionsCount(1)
            .setClusterSize(3)
            .setReplicationFactor(3)
            .setCommandApiAddress("localhost:26503")
            .setTopologyZone("zone-a");

    final var topology =
        BrokerClientTopologyImpl.fromMemberProperties(
            List.of(broker0, broker1, broker2),
            new BrokerClientTopologyImpl.ConfiguredClusterState(
                3, 1, 3, List.of(1), -1, "test", 0));

    // then
    assertThat(topology.getBrokerZone(0)).isEqualTo("zone-a");
    assertThat(topology.getBrokerZone(1)).isEqualTo("zone-b");
    assertThat(topology.getBrokerZone(2)).isEqualTo("zone-a");
  }
}
