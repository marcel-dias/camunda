/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.dynamic.config.util;

import io.camunda.zeebe.dynamic.config.TopologyConstraints;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates the topology configuration and produces warnings for suboptimal setups. Called during
 * broker startup to help operators identify misconfigurations.
 */
public final class TopologyValidator {

  private static final Logger LOG = LoggerFactory.getLogger(TopologyValidator.class);

  private TopologyValidator() {}

  /**
   * Validates topology constraints and returns a list of warnings. Also logs the warnings at WARN
   * level and logs a summary at INFO level.
   */
  public static List<String> validate(
      final TopologyConstraints constraints, final int replicationFactor) {
    final var warnings = new ArrayList<String>();

    if (!constraints.isTopologyAware()) {
      LOG.info(
          "Topology awareness is not configured. Partitions will be distributed without zone constraints.");
      return warnings;
    }

    final var zones = constraints.getZones();
    LOG.info("Topology-aware distribution enabled with {} zone(s): {}", zones.size(), zones);

    // Warn if RF > 1 but only one zone
    if (replicationFactor > 1 && zones.size() == 1) {
      final var msg =
          String.format(
              "Replication factor is %d but all brokers are in a single zone '%s'. "
                  + "This provides no zone-level fault tolerance. "
                  + "Consider distributing brokers across multiple zones.",
              replicationFactor, zones.iterator().next());
      warnings.add(msg);
      LOG.warn(msg);
    }

    // Warn if RF > zone count (some zones will have multiple replicas)
    if (replicationFactor > zones.size() && zones.size() > 1) {
      final var msg =
          String.format(
              "Replication factor (%d) exceeds zone count (%d). "
                  + "Some zones will host multiple replicas of the same partition.",
              replicationFactor, zones.size());
      warnings.add(msg);
      LOG.warn(msg);
    }

    // Log zone distribution summary
    for (final var zone : zones) {
      final var membersInZone = constraints.getMembersInZone(zone);
      LOG.info("Zone '{}': {} member(s) - {}", zone, membersInZone.size(), membersInZone);
    }

    return warnings;
  }
}
