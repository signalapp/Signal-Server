/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisClusterUtil {

    private static final String[] HASHES_BY_SLOT = new String[SlotHash.SLOT_COUNT];

    static {
        int slotsCovered = 0;
        int i = 0;

        while (slotsCovered < HASHES_BY_SLOT.length) {
            final String hash = Integer.toString(i++, 36);
            final int slot = SlotHash.getSlot(hash);

            if (HASHES_BY_SLOT[slot] == null) {
                HASHES_BY_SLOT[slot] = hash;
                slotsCovered += 1;
            }
        }
    }

    /**
     * Returns a Redis hash tag that maps to the given cluster slot.
     *
     * @param slot the Redis cluster slot for which to retrieve a hash tag
     *
     * @return a Redis hash tag that maps to the given cluster slot
     *
     * @see <a href="https://redis.io/topics/cluster-spec#keys-hash-tags">Redis Cluster Specification - Keys hash tags</a>
     */
    public static String getMinimalHashTag(final int slot) {
        return HASHES_BY_SLOT[slot];
    }

  /**
   * Returns an array indicating which slots have moved as part of a {@link ClusterTopologyChangedEvent}. The elements
   * of the array map to slots in the cluster; for example, if slot 1234 has changed, then element 1234 of the returned
   * array will be {@code true}.
   *
   * @param clusterTopologyChangedEvent the event from which to derive an array of changed slots
   *
   * @return an array indicating which slots of changed
   */
  public static boolean[] getChangedSlots(final ClusterTopologyChangedEvent clusterTopologyChangedEvent) {
      final Map<String, RedisClusterNode> beforeNodesById = clusterTopologyChangedEvent.before().stream()
          .collect(Collectors.toMap(RedisClusterNode::getNodeId, node -> node));

      final Map<String, RedisClusterNode> afterNodesById = clusterTopologyChangedEvent.after().stream()
          .collect(Collectors.toMap(RedisClusterNode::getNodeId, node -> node));

      final Set<String> nodeIds = new HashSet<>(beforeNodesById.keySet());
      nodeIds.addAll(afterNodesById.keySet());

      final boolean[] changedSlots = new boolean[SlotHash.SLOT_COUNT];

      for (final String nodeId : nodeIds) {
        if (beforeNodesById.containsKey(nodeId) && afterNodesById.containsKey(nodeId)) {
          // This node was present before and after the topology change, but its slots may have changed
          final boolean[] beforeSlots = new boolean[SlotHash.SLOT_COUNT];
          beforeNodesById.get(nodeId).getSlots().forEach(slot -> beforeSlots[slot] = true);

          final boolean[] afterSlots = new boolean[SlotHash.SLOT_COUNT];
          afterNodesById.get(nodeId).getSlots().forEach(slot -> afterSlots[slot] = true);

          for (int slot = 0; slot < SlotHash.SLOT_COUNT; slot++) {
            changedSlots[slot] |= beforeSlots[slot] ^ afterSlots[slot];
          }
        } else if (beforeNodesById.containsKey(nodeId)) {
          // The node was present before the topology change, but is gone now; all of its slots should be considered
          // changed
          beforeNodesById.get(nodeId).getSlots().forEach(slot -> changedSlots[slot] = true);
        } else {
          // The node was present after the change, but wasn't there before; all of its slots should be considered
          // changed
          afterNodesById.get(nodeId).getSlots().forEach(slot -> changedSlots[slot] = true);
        }
      }

      return changedSlots;
    }
}
