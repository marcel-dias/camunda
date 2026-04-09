/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TopologyInfoTest {

  @Test
  void shouldCreateWithZoneAndRegion() {
    // given
    final var topology = new TopologyInfo();
    topology.setZone("us-east-1a");
    topology.setRegion("us-east-1");

    // then
    assertThat(topology.getZone()).isEqualTo("us-east-1a");
    assertThat(topology.getRegion()).isEqualTo("us-east-1");
    assertThat(topology.isConfigured()).isTrue();
  }

  @Test
  void shouldReportNotConfiguredWhenEmpty() {
    // given
    final var topology = new TopologyInfo();

    // then
    assertThat(topology.isConfigured()).isFalse();
    assertThat(topology.getZone()).isNull();
    assertThat(topology.getRegion()).isNull();
  }

  @Test
  void shouldBeConfiguredWithZoneOnly() {
    // given
    final var topology = new TopologyInfo();
    topology.setZone("zone-a");

    // then
    assertThat(topology.isConfigured()).isTrue();
  }

  @Test
  void shouldImplementEqualsAndHashCode() {
    // given
    final var t1 = new TopologyInfo();
    t1.setZone("zone-a");
    t1.setRegion("region-1");

    final var t2 = new TopologyInfo();
    t2.setZone("zone-a");
    t2.setRegion("region-1");

    // then
    assertThat(t1).isEqualTo(t2);
    assertThat(t1.hashCode()).isEqualTo(t2.hashCode());
  }
}
