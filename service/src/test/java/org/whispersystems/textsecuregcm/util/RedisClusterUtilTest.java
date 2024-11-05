/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.event.ClusterTopologyChangedEvent;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

class RedisClusterUtilTest {

  @Test
  void testGetMinimalHashTag() {
    for (int slot = 0; slot < SlotHash.SLOT_COUNT; slot++) {
      assertEquals(slot, SlotHash.getSlot(RedisClusterUtil.getMinimalHashTag(slot)));
    }
  }

  @ParameterizedTest
  @MethodSource
  void getChangedSlots(final ClusterTopologyChangedEvent event, final boolean[] expectedSlotsChanged) {
    assertArrayEquals(expectedSlotsChanged, RedisClusterUtil.getChangedSlots(event));
  }

  private static List<Arguments> getChangedSlots() {
    final List<Arguments> arguments = new ArrayList<>();

    // Slot moved from one node to another
    {
      final String firstNodeId = UUID.randomUUID().toString();
      final String secondNodeId = UUID.randomUUID().toString();

      final RedisClusterNode firstNodeBefore = mock(RedisClusterNode.class);
      when(firstNodeBefore.getNodeId()).thenReturn(firstNodeId);
      when(firstNodeBefore.getSlots()).thenReturn(getSlotRange(0, 8192));

      final RedisClusterNode secondNodeBefore = mock(RedisClusterNode.class);
      when(secondNodeBefore.getNodeId()).thenReturn(secondNodeId);
      when(secondNodeBefore.getSlots()).thenReturn(getSlotRange(8192, 16384));

      final RedisClusterNode firstNodeAfter = mock(RedisClusterNode.class);
      when(firstNodeAfter.getNodeId()).thenReturn(firstNodeId);
      when(firstNodeAfter.getSlots()).thenReturn(getSlotRange(0, 8191));

      final RedisClusterNode secondNodeAfter = mock(RedisClusterNode.class);
      when(secondNodeAfter.getNodeId()).thenReturn(secondNodeId);
      when(secondNodeAfter.getSlots()).thenReturn(getSlotRange(8191, 16384));

      final ClusterTopologyChangedEvent clusterTopologyChangedEvent = new ClusterTopologyChangedEvent(
          List.of(firstNodeBefore, secondNodeBefore),
          List.of(firstNodeAfter, secondNodeAfter));

      final boolean[] slotsChanged = new boolean[SlotHash.SLOT_COUNT];
      slotsChanged[8191] = true;

      arguments.add(Arguments.of(clusterTopologyChangedEvent, slotsChanged));
    }

    // New node added to cluster
    {
      final String firstNodeId = UUID.randomUUID().toString();
      final String secondNodeId = UUID.randomUUID().toString();

      final RedisClusterNode firstNodeBefore = mock(RedisClusterNode.class);
      when(firstNodeBefore.getNodeId()).thenReturn(firstNodeId);
      when(firstNodeBefore.getSlots()).thenReturn(getSlotRange(0, 8192));

      final RedisClusterNode secondNodeBefore = mock(RedisClusterNode.class);
      when(secondNodeBefore.getNodeId()).thenReturn(secondNodeId);
      when(secondNodeBefore.getSlots()).thenReturn(getSlotRange(8192, 16384));

      final RedisClusterNode firstNodeAfter = mock(RedisClusterNode.class);
      when(firstNodeAfter.getNodeId()).thenReturn(firstNodeId);
      when(firstNodeAfter.getSlots()).thenReturn(getSlotRange(0, 8192));

      final RedisClusterNode secondNodeAfter = mock(RedisClusterNode.class);
      when(secondNodeAfter.getNodeId()).thenReturn(secondNodeId);
      when(secondNodeAfter.getSlots()).thenReturn(getSlotRange(8192, 12288));

      final RedisClusterNode thirdNodeAfter = mock(RedisClusterNode.class);
      when(thirdNodeAfter.getNodeId()).thenReturn(UUID.randomUUID().toString());
      when(thirdNodeAfter.getSlots()).thenReturn(getSlotRange(12288, 16384));

      final ClusterTopologyChangedEvent clusterTopologyChangedEvent = new ClusterTopologyChangedEvent(
          List.of(firstNodeBefore, secondNodeBefore),
          List.of(firstNodeAfter, secondNodeAfter, thirdNodeAfter));

      final boolean[] slotsChanged = new boolean[SlotHash.SLOT_COUNT];

      for (int slot = 12288; slot < 16384; slot++) {
        slotsChanged[slot] = true;
      }

      arguments.add(Arguments.of(clusterTopologyChangedEvent, slotsChanged));
    }

    // Node removed from cluster
    {
      final String firstNodeId = UUID.randomUUID().toString();
      final String secondNodeId = UUID.randomUUID().toString();

      final RedisClusterNode firstNodeBefore = mock(RedisClusterNode.class);
      when(firstNodeBefore.getNodeId()).thenReturn(firstNodeId);
      when(firstNodeBefore.getSlots()).thenReturn(getSlotRange(0, 8192));

      final RedisClusterNode secondNodeBefore = mock(RedisClusterNode.class);
      when(secondNodeBefore.getNodeId()).thenReturn(secondNodeId);
      when(secondNodeBefore.getSlots()).thenReturn(getSlotRange(8192, 12288));

      final RedisClusterNode thirdNodeBefore = mock(RedisClusterNode.class);
      when(thirdNodeBefore.getNodeId()).thenReturn(UUID.randomUUID().toString());
      when(thirdNodeBefore.getSlots()).thenReturn(getSlotRange(12288, 16384));

      final RedisClusterNode firstNodeAfter = mock(RedisClusterNode.class);
      when(firstNodeAfter.getNodeId()).thenReturn(firstNodeId);
      when(firstNodeAfter.getSlots()).thenReturn(getSlotRange(0, 8192));

      final RedisClusterNode secondNodeAfter = mock(RedisClusterNode.class);
      when(secondNodeAfter.getNodeId()).thenReturn(secondNodeId);
      when(secondNodeAfter.getSlots()).thenReturn(getSlotRange(8192, 16384));

      final ClusterTopologyChangedEvent clusterTopologyChangedEvent = new ClusterTopologyChangedEvent(
          List.of(firstNodeBefore, secondNodeBefore, thirdNodeBefore),
          List.of(firstNodeAfter, secondNodeAfter));

      final boolean[] slotsChanged = new boolean[SlotHash.SLOT_COUNT];

      for (int slot = 12288; slot < 16384; slot++) {
        slotsChanged[slot] = true;
      }

      arguments.add(Arguments.of(clusterTopologyChangedEvent, slotsChanged));
    }

    // Node added, node removed, and slot moved
    // Node removed from cluster
    {
      final String secondNodeId = UUID.randomUUID().toString();
      final String thirdNodeId = UUID.randomUUID().toString();

      final RedisClusterNode firstNodeBefore = mock(RedisClusterNode.class);
      when(firstNodeBefore.getNodeId()).thenReturn(UUID.randomUUID().toString());
      when(firstNodeBefore.getSlots()).thenReturn(getSlotRange(0, 1));

      final RedisClusterNode secondNodeBefore = mock(RedisClusterNode.class);
      when(secondNodeBefore.getNodeId()).thenReturn(secondNodeId);
      when(secondNodeBefore.getSlots()).thenReturn(getSlotRange(1, 8192));

      final RedisClusterNode thirdNodeBefore = mock(RedisClusterNode.class);
      when(thirdNodeBefore.getNodeId()).thenReturn(thirdNodeId);
      when(thirdNodeBefore.getSlots()).thenReturn(getSlotRange(8192, 16384));

      final RedisClusterNode secondNodeAfter = mock(RedisClusterNode.class);
      when(secondNodeAfter.getNodeId()).thenReturn(secondNodeId);
      when(secondNodeAfter.getSlots()).thenReturn(getSlotRange(0, 8191));

      final RedisClusterNode thirdNodeAfter = mock(RedisClusterNode.class);
      when(thirdNodeAfter.getNodeId()).thenReturn(thirdNodeId);
      when(thirdNodeAfter.getSlots()).thenReturn(getSlotRange(8191, 16383));

      final RedisClusterNode fourthNodeAfter = mock(RedisClusterNode.class);
      when(fourthNodeAfter.getNodeId()).thenReturn(UUID.randomUUID().toString());
      when(fourthNodeAfter.getSlots()).thenReturn(getSlotRange(16383, 16384));

      final ClusterTopologyChangedEvent clusterTopologyChangedEvent = new ClusterTopologyChangedEvent(
          List.of(firstNodeBefore, secondNodeBefore, thirdNodeBefore),
          List.of(secondNodeAfter, thirdNodeAfter, fourthNodeAfter));

      final boolean[] slotsChanged = new boolean[SlotHash.SLOT_COUNT];
      slotsChanged[0] = true;
      slotsChanged[8191] = true;
      slotsChanged[16383] = true;

      arguments.add(Arguments.of(clusterTopologyChangedEvent, slotsChanged));
    }

    return arguments;
  }

  private static List<Integer> getSlotRange(final int startInclusive, final int endExclusive) {
    final List<Integer> slots = new ArrayList<>(endExclusive - startInclusive);

    for (int i = startInclusive; i < endExclusive; i++) {
      slots.add(i);
    }

    return slots;
  }
}
