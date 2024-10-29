/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequest;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequestContext;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.tests.util.ExperimentHelper;

public class BackupAuthTestUtil {

  final GenericServerSecretParams params = GenericServerSecretParams.generate();
  final Clock clock;

  public BackupAuthTestUtil(final Clock clock) {
    this.clock = clock;
  }

  public BackupAuthCredentialRequest getRequest(final byte[] backupKey, final UUID aci) {
    return BackupAuthCredentialRequestContext.create(backupKey, aci).getRequest();
  }

  public BackupAuthCredentialPresentation getPresentation(
      final BackupLevel backupLevel, final byte[] backupKey, final UUID aci)
      throws VerificationFailedException {
    return getPresentation(params, backupLevel, backupKey, aci);
  }

  public BackupAuthCredentialPresentation getPresentation(
      GenericServerSecretParams params, final BackupLevel backupLevel, final byte[] backupKey, final UUID aci)
      throws VerificationFailedException {
    final Instant redemptionTime = clock.instant().truncatedTo(ChronoUnit.DAYS);
    final BackupAuthCredentialRequestContext ctx = BackupAuthCredentialRequestContext.create(backupKey, aci);
    return ctx.receiveResponse(
            ctx.getRequest()
                .issueCredential(clock.instant().truncatedTo(ChronoUnit.DAYS), backupLevel, BackupCredentialType.MESSAGES, params),
            redemptionTime,
            params.getPublicParams())
        .present(params.getPublicParams());
  }

  public List<BackupAuthManager.Credential> getCredentials(
      final BackupLevel backupLevel,
      final BackupAuthCredentialRequest request,
      final BackupCredentialType credentialType,
      final Instant redemptionStart,
      final Instant redemptionEnd) {
    final UUID aci = UUID.randomUUID();

    final String experimentName = switch (backupLevel) {
      case FREE -> BackupAuthManager.BACKUP_EXPERIMENT_NAME;
      case PAID -> BackupAuthManager.BACKUP_MEDIA_EXPERIMENT_NAME;
    };
    final BackupAuthManager issuer = new BackupAuthManager(
        ExperimentHelper.withEnrollment(experimentName, aci), null, null, null, null, params, clock);
    Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(aci);
    when(account.getBackupCredentialRequest(credentialType)).thenReturn(Optional.of(request.serialize()));
    return issuer.getBackupAuthCredentials(account, credentialType, redemptionStart, redemptionEnd).join();
  }
}
