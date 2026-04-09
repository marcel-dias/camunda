/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.dynamic.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.cluster.MemberId;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TopologyConstraintsTest {

  @Test
  void shouldGroupMembersByZone() {
    // given
    final var constraints =
        new TopologyConstraints(
            Map.of(
                MemberId.from("0"), "zone-a",
                MemberId.from("1"), "zone-b",
                MemberId.from("2"), "zone-a",
                MemberId.from("3"), "zone-b"));

    // then
    assertThat(constraints.getZones()).containsExactlyInAnyOrder("zone-a", "zone-b");
    assertThat(constraints.getMembersInZone("zone-a"))
        .containsExactlyInAnyOrder(MemberId.from("0"), MemberId.from("2"));
    assertThat(constraints.getMembersInZone("zone-b"))
        .containsExactlyInAnyOrder(MemberId.from("1"), MemberId.from("3"));
  }

  @Test
  void shouldReturnZoneForMember() {
    // given
    final var constraints = new TopologyConstraints(Map.of(MemberId.from("0"), "zone-a"));

    // then
    assertThat(constraints.getZoneForMember(MemberId.from("0"))).hasValue("zone-a");
    assertThat(constraints.getZoneForMember(MemberId.from("99"))).isEmpty();
  }

  @Test
  void shouldDetectUnconfiguredTopology() {
    // given
    final var empty = TopologyConstraints.unconstrained();

    // then
    assertThat(empty.isTopologyAware()).isFalse();
  }

  @Test
  void shouldGroupMembersByRegion() {
    // given
    final var constraints =
        new TopologyConstraints(
            Map.of(MemberId.from("0"), "zone-a", MemberId.from("1"), "zone-b"),
            Map.of(MemberId.from("0"), "us-east", MemberId.from("1"), "eu-west"));

    // when/then
    assertThat(constraints.isRegionAware()).isTrue();
    assertThat(constraints.getRegions()).containsExactlyInAnyOrder("us-east", "eu-west");
    assertThat(constraints.getMembersInRegion("us-east")).containsExactly(MemberId.from("0"));
    assertThat(constraints.getRegionForMember(MemberId.from("1"))).contains("eu-west");
    assertThat(constraints.regionCount()).isEqualTo(2);
  }

  @Test
  void shouldNotBeRegionAwareWithoutRegionData() {
    // given
    final var constraints = new TopologyConstraints(Map.of(MemberId.from("0"), "zone-a"));

    // when/then
    assertThat(constraints.isRegionAware()).isFalse();
    assertThat(constraints.getRegions()).isEmpty();
  }
}
