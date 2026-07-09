/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.foundationdb.FoundationDbMessageStream;

class MirroringMessageStreamTest {

  private FoundationDbMessageStream foundationDbMessageStream;
  private ExperimentEnrollmentManager experimentEnrollmentManager;

  private MirroringMessageStream mirroringMessageStream;

  private static final AciServiceIdentifier ACCOUNT_IDENTIFIER = new AciServiceIdentifier(UUID.randomUUID());
  private static final byte DEVICE_ID = Device.PRIMARY_ID;

  @BeforeEach
  void setUp() {
    foundationDbMessageStream = mock(FoundationDbMessageStream.class);
    experimentEnrollmentManager = mock(ExperimentEnrollmentManager.class);

    mirroringMessageStream = new MirroringMessageStream(
        mock(RedisDynamoDbMessageStream.class),
        foundationDbMessageStream,
        experimentEnrollmentManager,
        ACCOUNT_IDENTIFIER.uuid(),
        DEVICE_ID);
  }

  @ParameterizedTest
  @MethodSource
  void acknowledgeMessage(final boolean enrolled, final UUID messageGuid, final boolean expectFoundationDbDeletion) {
    when(experimentEnrollmentManager.isEnrolled(any(UUID.class), eq(MessagesManager.MIRROR_DELETIONS_EXPERIMENT_NAME)))
        .thenReturn(enrolled);

    final long serverTimestamp = System.currentTimeMillis();

    mirroringMessageStream.acknowledgeMessage(messageGuid, serverTimestamp);

    if (expectFoundationDbDeletion) {
      verify(foundationDbMessageStream).acknowledgeMessage(messageGuid, serverTimestamp);
    } else {
      verify(foundationDbMessageStream, never()).acknowledgeMessage(any(), anyLong());
    }
  }

  private static List<Arguments> acknowledgeMessage() {
    return List.of(
        Arguments.argumentSet("Not enrolled, v4 UUID", false, UUID.randomUUID(), false),
        Arguments.argumentSet("Not enrolled, v8 UUID", false, MessageGuidUtil.generateRandomV8UUID(), false),
        Arguments.argumentSet("Enrolled, v4 UUID", true, UUID.randomUUID(), false),
        Arguments.argumentSet("Enrolled, v8 UUID", true, MessageGuidUtil.generateRandomV8UUID(), true)
    );
  }
}
