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
import io.camunda.zeebe.dynamic.config.state.ClusterConfiguration;
import io.camunda.zeebe.dynamic.config.state.MemberState;
import io.camunda.zeebe.dynamic.config.state.TopologyInfo;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TopologyConstraintsFactoryTest {

  @Test
  void shouldBuildConstraintsFromClusterConfiguration() {
    // given
    final var zoneA = new TopologyInfo();
    zoneA.setZone("zone-a");
    final var zoneB = new TopologyInfo();
    zoneB.setZone("zone-b");

    final var config =
        ClusterConfiguration.init()
            .addMember(MemberId.from("0"), MemberState.initializeAsActive(Map.of(), zoneA))
            .addMember(MemberId.from("1"), MemberState.initializeAsActive(Map.of(), zoneB))
            .addMember(MemberId.from("2"), MemberState.initializeAsActive(Map.of(), zoneA));

    // when
    final var constraints = TopologyConstraintsFactory.fromClusterConfiguration(config);

    // then
    assertThat(constraints.isTopologyAware()).isTrue();
    assertThat(constraints.getZones()).containsExactlyInAnyOrder("zone-a", "zone-b");
    assertThat(constraints.getMembersInZone("zone-a"))
        .containsExactlyInAnyOrder(MemberId.from("0"), MemberId.from("2"));
    assertThat(constraints.getMembersInZone("zone-b"))
        .containsExactlyInAnyOrder(MemberId.from("1"));
  }

  @Test
  void shouldReturnUnconstrainedWhenNoTopologyConfigured() {
    // given
    final var config =
        ClusterConfiguration.init()
            .addMember(MemberId.from("0"), MemberState.initializeAsActive(Map.of()))
            .addMember(MemberId.from("1"), MemberState.initializeAsActive(Map.of()));

    // when
    final var constraints = TopologyConstraintsFactory.fromClusterConfiguration(config);

    // then
    assertThat(constraints.isTopologyAware()).isFalse();
  }

  @Test
  void shouldReturnUnconstrainedWhenOnlySomeMembersHaveTopology() {
    // given — mixed: some have topology, some don't
    final var zoneA = new TopologyInfo();
    zoneA.setZone("zone-a");

    final var config =
        ClusterConfiguration.init()
            .addMember(MemberId.from("0"), MemberState.initializeAsActive(Map.of(), zoneA))
            .addMember(MemberId.from("1"), MemberState.initializeAsActive(Map.of()));

    // when
    final var constraints = TopologyConstraintsFactory.fromClusterConfiguration(config);

    // then — unconstrained because not all members have topology
    assertThat(constraints.isTopologyAware()).isFalse();
  }

  @Test
  void shouldIgnoreLeavingAndLeftMembers() {
    // given — one active with zone, one LEFT without zone
    final var zoneA = new TopologyInfo();
    zoneA.setZone("zone-a");

    final var config =
        ClusterConfiguration.init()
            .addMember(MemberId.from("0"), MemberState.initializeAsActive(Map.of(), zoneA));

    // when
    final var constraints = TopologyConstraintsFactory.fromClusterConfiguration(config);

    // then — should be topology aware (LEFT members are ignored)
    assertThat(constraints.isTopologyAware()).isTrue();
    assertThat(constraints.getZones()).containsExactly("zone-a");
  }

  @Test
  void shouldReturnUnconstrainedForEmptyCluster() {
    // given
    final var config = ClusterConfiguration.init();

    // when
    final var constraints = TopologyConstraintsFactory.fromClusterConfiguration(config);

    // then
    assertThat(constraints.isTopologyAware()).isFalse();
  }
}
