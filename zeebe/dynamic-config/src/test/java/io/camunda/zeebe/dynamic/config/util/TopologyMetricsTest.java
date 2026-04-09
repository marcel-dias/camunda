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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TopologyMetricsTest {

  @Test
  void shouldReportTopologyNotConfiguredWhenUnconstrained() {
    // given
    final var registry = new SimpleMeterRegistry();
    final var metrics = new TopologyMetrics(registry);

    // when — no update called, defaults to unconstrained

    // then
    final var gauge = registry.get(TopologyMetrics.METRIC_TOPOLOGY_CONFIGURED).gauge();
    assertThat(gauge.value()).isEqualTo(0.0);
  }

  @Test
  void shouldReportTopologyConfiguredAfterUpdate() {
    // given
    final var registry = new SimpleMeterRegistry();
    final var metrics = new TopologyMetrics(registry);
    final var constraints =
        new TopologyConstraints(
            Map.of(
                MemberId.from("0"), "zone-a",
                MemberId.from("1"), "zone-b"));

    // when
    metrics.updateMetrics(constraints);

    // then
    final var gauge = registry.get(TopologyMetrics.METRIC_TOPOLOGY_CONFIGURED).gauge();
    assertThat(gauge.value()).isEqualTo(1.0);
  }

  @Test
  void shouldRegisterZoneMemberGauges() {
    // given
    final var registry = new SimpleMeterRegistry();
    final var metrics = new TopologyMetrics(registry);
    final var constraints =
        new TopologyConstraints(
            Map.of(
                MemberId.from("0"), "zone-a",
                MemberId.from("1"), "zone-a",
                MemberId.from("2"), "zone-b"));

    // when
    metrics.updateMetrics(constraints);

    // then
    final var zoneAGauge =
        registry
            .get(TopologyMetrics.METRIC_ZONE_MEMBERS)
            .tag(TopologyMetrics.TAG_ZONE, "zone-a")
            .gauge();
    final var zoneBGauge =
        registry
            .get(TopologyMetrics.METRIC_ZONE_MEMBERS)
            .tag(TopologyMetrics.TAG_ZONE, "zone-b")
            .gauge();
    assertThat(zoneAGauge.value()).isEqualTo(2.0);
    assertThat(zoneBGauge.value()).isEqualTo(1.0);
  }
}
