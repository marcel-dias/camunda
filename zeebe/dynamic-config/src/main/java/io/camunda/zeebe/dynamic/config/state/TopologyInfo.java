/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.dynamic.config.state;

import java.util.Objects;

/**
 * Topology information for a broker node, describing its placement in the infrastructure
 * (availability zone and region). Used for topology-aware partition distribution and leader
 * election.
 */
public class TopologyInfo {

  private String zone;
  private String region;

  public String getZone() {
    return zone;
  }

  public void setZone(final String zone) {
    this.zone = zone;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(final String region) {
    this.region = region;
  }

  /** Returns true if at least zone or region is set. */
  public boolean isConfigured() {
    return zone != null || region != null;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TopologyInfo that = (TopologyInfo) o;
    return Objects.equals(zone, that.zone) && Objects.equals(region, that.region);
  }

  @Override
  public int hashCode() {
    return Objects.hash(zone, region);
  }

  @Override
  public String toString() {
    return "TopologyInfo{zone='" + zone + "', region='" + region + "'}";
  }
}
