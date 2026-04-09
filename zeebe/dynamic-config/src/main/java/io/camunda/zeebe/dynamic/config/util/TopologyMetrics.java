/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.dynamic.config.util;

import io.camunda.zeebe.dynamic.config.TopologyConstraints;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Registers and updates topology distribution metrics. Call {@link
 * #updateMetrics(TopologyConstraints)} whenever the cluster topology changes.
 */
public final class TopologyMetrics {

  static final String METRIC_ZONE_MEMBERS = "zeebe.cluster.topology.zone.members";
  static final String METRIC_TOPOLOGY_CONFIGURED = "zeebe.cluster.topology.configured";
  static final String TAG_ZONE = "zone";

  private final MeterRegistry registry;
  private final AtomicReference<TopologyConstraints> currentConstraints =
      new AtomicReference<>(TopologyConstraints.unconstrained());
  private final Map<String, Gauge> zoneGauges = new ConcurrentHashMap<>();

  public TopologyMetrics(final MeterRegistry registry) {
    this.registry = registry;
    Gauge.builder(METRIC_TOPOLOGY_CONFIGURED, this, m -> m.isTopologyConfigured() ? 1.0 : 0.0)
        .description("Whether topology-aware distribution is configured (1=yes, 0=no)")
        .register(registry);
  }

  public void updateMetrics(final TopologyConstraints constraints) {
    currentConstraints.set(constraints);

    if (constraints.isTopologyAware()) {
      for (final var zone : constraints.getZones()) {
        zoneGauges.computeIfAbsent(
            zone,
            z ->
                Gauge.builder(METRIC_ZONE_MEMBERS, constraints, c -> c.getMembersInZone(z).size())
                    .description("Number of cluster members in this zone")
                    .tags(Tags.of(TAG_ZONE, z))
                    .register(registry));
      }
    }
  }

  private boolean isTopologyConfigured() {
    return currentConstraints.get().isTopologyAware();
  }
}
