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
import io.camunda.zeebe.dynamic.config.TopologyConstraints;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TopologyValidatorTest {

  @Test
  void shouldReturnNoWarningsForHealthyMultiZoneSetup() {
    // given — 3 zones, RF=3
    final var constraints =
        new TopologyConstraints(
            Map.of(
                MemberId.from("0"), "zone-a",
                MemberId.from("1"), "zone-b",
                MemberId.from("2"), "zone-c"));

    // when
    final var warnings = TopologyValidator.validate(constraints, 3);

    // then
    assertThat(warnings).isEmpty();
  }

  @Test
  void shouldWarnWhenAllBrokersInSingleZone() {
    // given — single zone, RF=3
    final var constraints =
        new TopologyConstraints(
            Map.of(
                MemberId.from("0"), "zone-a",
                MemberId.from("1"), "zone-a",
                MemberId.from("2"), "zone-a"));

    // when
    final var warnings = TopologyValidator.validate(constraints, 3);

    // then
    assertThat(warnings).hasSize(1);
    assertThat(warnings.get(0)).contains("single zone").contains("zone-a");
  }

  @Test
  void shouldWarnWhenRFExceedsZoneCount() {
    // given — 2 zones, RF=3
    final var constraints =
        new TopologyConstraints(
            Map.of(
                MemberId.from("0"), "zone-a",
                MemberId.from("1"), "zone-b",
                MemberId.from("2"), "zone-a"));

    // when
    final var warnings = TopologyValidator.validate(constraints, 3);

    // then
    assertThat(warnings).hasSize(1);
    assertThat(warnings.get(0)).contains("exceeds zone count");
  }

  @Test
  void shouldReturnNoWarningsWhenTopologyNotConfigured() {
    // given
    final var constraints = TopologyConstraints.unconstrained();

    // when
    final var warnings = TopologyValidator.validate(constraints, 3);

    // then
    assertThat(warnings).isEmpty();
  }

  @Test
  void shouldNotWarnForSingleZoneWithRF1() {
    // given — single zone, RF=1 (no redundancy expected)
    final var constraints = new TopologyConstraints(Map.of(MemberId.from("0"), "zone-a"));

    // when
    final var warnings = TopologyValidator.validate(constraints, 1);

    // then
    assertThat(warnings).isEmpty();
  }
}
