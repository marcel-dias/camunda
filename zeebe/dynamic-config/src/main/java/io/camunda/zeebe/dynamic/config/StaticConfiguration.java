/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.dynamic.config;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionMetadata;
import io.camunda.zeebe.dynamic.config.state.ClusterConfiguration;
import io.camunda.zeebe.dynamic.config.state.DynamicPartitionConfig;
import io.camunda.zeebe.dynamic.config.state.TopologyInfo;
import io.camunda.zeebe.dynamic.config.util.ConfigurationUtil;
import java.util.List;
import java.util.Set;

public record StaticConfiguration(
    PartitionDistributor partitionDistributor,
    Set<MemberId> clusterMembers,
    MemberId localMemberId,
    List<PartitionId> partitionIds,
    int replicationFactor,
    DynamicPartitionConfig partitionConfig,
    String clusterId,
    TopologyInfo localTopology) {

  /** Backward-compatible constructor without topology. */
  public StaticConfiguration(
      final PartitionDistributor partitionDistributor,
      final Set<MemberId> clusterMembers,
      final MemberId localMemberId,
      final List<PartitionId> partitionIds,
      final int replicationFactor,
      final DynamicPartitionConfig partitionConfig,
      final String clusterId) {
    this(
        partitionDistributor,
        clusterMembers,
        localMemberId,
        partitionIds,
        replicationFactor,
        partitionConfig,
        clusterId,
        new TopologyInfo());
  }

  public int partitionCount() {
    return partitionIds.size();
  }

  public ClusterConfiguration generateTopology() {
    final Set<PartitionMetadata> partitionDistribution = generatePartitionDistribution();
    return ConfigurationUtil.getClusterConfigFrom(
        partitionDistribution, partitionConfig, clusterId, localTopology);
  }

  public Set<PartitionMetadata> generatePartitionDistribution() {
    final var sortedPartitionIds = partitionIds.stream().sorted().toList();
    return partitionDistributor.distributePartitions(
        clusterMembers, sortedPartitionIds, replicationFactor);
  }
}
