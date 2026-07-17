/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.signal.chat.account.Capabilities;
import org.signal.chat.account.ChangeNumberRequest;
import org.signal.chat.account.ChangeNumberResponse;
import org.signal.chat.account.ClearRegistrationLockRequest;
import org.signal.chat.account.ClearRegistrationLockResponse;
import org.signal.chat.account.ConfigureUnidentifiedAccessRequest;
import org.signal.chat.account.ConfigureUnidentifiedAccessResponse;
import org.signal.chat.account.ConfirmUsernameHashRequest;
import org.signal.chat.account.ConfirmUsernameHashResponse;
import org.signal.chat.account.DeleteAccountRequest;
import org.signal.chat.account.DeleteAccountResponse;
import org.signal.chat.account.DeleteUsernameHashRequest;
import org.signal.chat.account.DeleteUsernameHashResponse;
import org.signal.chat.account.DeleteUsernameLinkRequest;
import org.signal.chat.account.DeleteUsernameLinkResponse;
import org.signal.chat.account.ExternalServiceCredentials;
import org.signal.chat.account.GetAccountDataReportRequest;
import org.signal.chat.account.GetAccountDataReportResponse;
import org.signal.chat.account.GetAccountIdentityRequest;
import org.signal.chat.account.GetAccountIdentityResponse;
import org.signal.chat.account.GetCapabilitiesRequest;
import org.signal.chat.account.GetCapabilitiesResponse;
import org.signal.chat.account.GetEntitlementsRequest;
import org.signal.chat.account.GetEntitlementsResponse;
import org.signal.chat.account.RegistrationLockFailure;
import org.signal.chat.account.ReserveUsernameHashRequest;
import org.signal.chat.account.ReserveUsernameHashResponse;
import org.signal.chat.account.SetDiscoverableByPhoneNumberRequest;
import org.signal.chat.account.SetDiscoverableByPhoneNumberResponse;
import org.signal.chat.account.SetRegistrationLockRequest;
import org.signal.chat.account.SetRegistrationLockResponse;
import org.signal.chat.account.SetRegistrationRecoveryPasswordRequest;
import org.signal.chat.account.SetRegistrationRecoveryPasswordResponse;
import org.signal.chat.account.SetUsernameLinkRequest;
import org.signal.chat.account.SetUsernameLinkResponse;
import org.signal.chat.account.SetZkCredentialKeyRequest;
import org.signal.chat.account.SetZkCredentialKeyResponse;
import org.signal.chat.account.SimpleAccountsGrpc;
import org.signal.chat.account.StaleDevices;
import org.signal.chat.account.UsernameNotAvailable;
import org.signal.chat.common.AccountIdentifiers;
import org.signal.chat.errors.FailedPrecondition;
import org.signal.chat.errors.NotFound;
import org.signal.chat.messages.SendMessageType;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.usernames.BaseUsernameException;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ZkCredentialPublicKey;
import org.whispersystems.textsecuregcm.auth.InvalidRegistrationSessionException;
import org.whispersystems.textsecuregcm.auth.RecoveryPasswordVerificationFailedException;
import org.whispersystems.textsecuregcm.auth.RegistrationLockFailureException;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.auth.UnverifiedRegistrationSessionException;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.controllers.MessageDeliveryNotAllowedException;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.AccountDataReportResponse;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.MessageTooLargeException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberManager;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.UsernameHashNotAvailableException;
import org.whispersystems.textsecuregcm.storage.UsernameReservationNotFoundException;
import org.whispersystems.textsecuregcm.util.RegistrationIdValidator;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.UsernameHashZkProofVerifier;

public class AccountsGrpcService extends SimpleAccountsGrpc.AccountsImplBase {

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  private final AccountsManager accountsManager;
  private final RateLimiters rateLimiters;
  private final UsernameHashZkProofVerifier usernameHashZkProofVerifier;
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;
  private final Clock clock;
  private final ChangeNumberManager changeNumberManager;

  public AccountsGrpcService(final AccountsManager accountsManager,
      final RateLimiters rateLimiters,
      final UsernameHashZkProofVerifier usernameHashZkProofVerifier,
      final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
      final Clock clock,
      final ChangeNumberManager changeNumberManager) {

    this.accountsManager = accountsManager;
    this.rateLimiters = rateLimiters;
    this.usernameHashZkProofVerifier = usernameHashZkProofVerifier;
    this.registrationRecoveryPasswordsManager = registrationRecoveryPasswordsManager;
    this.clock = clock;
    this.changeNumberManager = changeNumberManager;
  }

  @Override
  public GetAccountIdentityResponse getAccountIdentity(final GetAccountIdentityRequest request) {
    return GetAccountIdentityResponse.newBuilder()
        .setAccountIdentifiers(buildAccountIdentifiers(getAuthenticatedAccount()))
        .build();
  }

  @Override
  public GetEntitlementsResponse getEntitlements(final GetEntitlementsRequest request) {
    final Account account = getAuthenticatedAccount();
    final GetEntitlementsResponse.Builder builder = GetEntitlementsResponse.newBuilder();

    final Instant now = clock.instant();

    final Account.BackupVoucher backupVoucher = account.getBackupVoucher();
    if (backupVoucher != null && backupVoucher.expiration().isAfter(now)) {
      builder.setBackup(GetEntitlementsResponse.BackupEntitlement.newBuilder()
          .setExpirationEpochSeconds(backupVoucher.expiration().getEpochSecond())
          .setLevel(backupVoucher.receiptLevel()));
    }

    builder.addAllBadges(account.getBadges().stream()
        .filter(badge -> badge.expiration().isAfter(now))
        .map(badge -> GetEntitlementsResponse.BadgeEntitlement.newBuilder()
            .setBadgeId(badge.id())
            .setExpirationEpochSeconds(badge.expiration().getEpochSecond())
            .setVisible(badge.visible())
            .build())
        .toList());

    return builder.build();
  }

  @Override
  public DeleteAccountResponse deleteAccount(final DeleteAccountRequest request) {
    accountsManager.delete(AuthenticationUtil.requireAuthenticatedDevice().accountIdentifier(),
        AccountsManager.DeletionReason.USER_REQUEST);

    return DeleteAccountResponse.getDefaultInstance();
  }

  @Override
  public SetRegistrationLockResponse setRegistrationLock(final SetRegistrationLockRequest request) {
    final SaltedTokenHash credentials =
        SaltedTokenHash.generateFor(formatRegistrationLock(request.getRegistrationLock().toByteArray()));

    accountsManager.update(AuthenticationUtil.requireAuthenticatedDevice().accountIdentifier(),
        account -> account.setRegistrationLock(credentials.hash(), credentials.salt()));

    return SetRegistrationLockResponse.getDefaultInstance();
  }

  @Override
  public ClearRegistrationLockResponse clearRegistrationLock(final ClearRegistrationLockRequest request) {
    accountsManager.update(AuthenticationUtil.requireAuthenticatedDevice().accountIdentifier(),
        account -> account.setRegistrationLock(null, null));

    return ClearRegistrationLockResponse.getDefaultInstance();
  }

  @Override
  public ReserveUsernameHashResponse reserveUsernameHash(final ReserveUsernameHashRequest request)
      throws RateLimitExceededException {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    final List<byte[]> usernameHashes = new ArrayList<>(request.getUsernameHashesCount());

    for (final ByteString usernameHash : request.getUsernameHashesList()) {
      usernameHashes.add(usernameHash.toByteArray());
    }

    rateLimiters.getUsernameReserveLimiter().validate(authenticatedDevice.accountIdentifier());

    try {
      final AccountsManager.UsernameReservation usernameReservation =
          accountsManager.reserveUsernameHash(authenticatedDevice.accountIdentifier(), usernameHashes);

      return ReserveUsernameHashResponse.newBuilder()
          .setUsernameHash(ByteString.copyFrom(usernameReservation.reservedUsernameHash()))
          .build();
    } catch (final UsernameHashNotAvailableException e) {
        return ReserveUsernameHashResponse.newBuilder()
            .setUsernameNotAvailable(UsernameNotAvailable.getDefaultInstance())
            .build();
    }
  }

  @Override
  public ConfirmUsernameHashResponse confirmUsernameHash(final ConfirmUsernameHashRequest request)
      throws RateLimitExceededException {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    try {
      usernameHashZkProofVerifier.verifyProof(request.getZkProof().toByteArray(), request.getUsernameHash().toByteArray());
    } catch (final BaseUsernameException e) {
      throw GrpcExceptions.invalidArguments("Could not verify proof");
    }

    rateLimiters.getUsernameSetLimiter().validate(authenticatedDevice.accountIdentifier());

    try {
      final Account updatedAccount = accountsManager.confirmReservedUsernameHash(authenticatedDevice.accountIdentifier(),
              request.getUsernameHash().toByteArray(),
              request.getUsernameCiphertext().toByteArray());

      return ConfirmUsernameHashResponse.newBuilder()
          .setConfirmedUsernameHash(ConfirmUsernameHashResponse.ConfirmedUsernameHash.newBuilder()
              .setUsernameHash(ByteString.copyFrom(updatedAccount.getUsernameHash().orElseThrow()))
              .setUsernameLinkHandle(UUIDUtil.toByteString(updatedAccount.getUsernameLinkHandle())))
          .build();
    } catch (final UsernameHashNotAvailableException e) {
      return ConfirmUsernameHashResponse
          .newBuilder()
          .setUsernameNotAvailable(UsernameNotAvailable.getDefaultInstance())
          .build();
    } catch (final UsernameReservationNotFoundException e) {
      return ConfirmUsernameHashResponse
          .newBuilder()
          .setReservationNotFound(FailedPrecondition.getDefaultInstance())
          .build();
    }
  }

  @Override
  public DeleteUsernameHashResponse deleteUsernameHash(final DeleteUsernameHashRequest request) {
    accountsManager.clearUsernameHash(AuthenticationUtil.requireAuthenticatedDevice().accountIdentifier());

    return DeleteUsernameHashResponse.getDefaultInstance();
  }

  @Override
  public SetUsernameLinkResponse setUsernameLink(final SetUsernameLinkRequest request)
      throws RateLimitExceededException {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    rateLimiters.getUsernameLinkOperationLimiter().validate(authenticatedDevice.accountIdentifier());

    final Account account = getAuthenticatedAccount();

    final SetUsernameLinkResponse.Builder responseBuilder = SetUsernameLinkResponse.newBuilder();

    if (account.getUsernameHash().isEmpty()) {
      return responseBuilder.setNoUsernameSet(FailedPrecondition.getDefaultInstance()).build();
    }

    final UUID linkHandle = (request.getKeepLinkHandle() && account.getUsernameLinkHandle() != null)
        ? account.getUsernameLinkHandle()
        : UUID.randomUUID();

    accountsManager.update(account.getIdentifier(IdentityType.ACI),
        a -> a.setUsernameLinkDetails(linkHandle, request.getUsernameCiphertext().toByteArray()));

    return responseBuilder.setUsernameLinkHandle(UUIDUtil.toByteString(linkHandle)).build();
  }

  @Override
  public DeleteUsernameLinkResponse deleteUsernameLink(final DeleteUsernameLinkRequest request)
      throws RateLimitExceededException {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    rateLimiters.getUsernameLinkOperationLimiter().validate(authenticatedDevice.accountIdentifier());

    accountsManager.update(authenticatedDevice.accountIdentifier(), a -> a.setUsernameLinkDetails(null, null));

    return DeleteUsernameLinkResponse.getDefaultInstance();
  }

  @Override
  public ConfigureUnidentifiedAccessResponse configureUnidentifiedAccess(final ConfigureUnidentifiedAccessRequest request) {
    if (request.getConfigurationCase() == ConfigureUnidentifiedAccessRequest.ConfigurationCase.CONFIGURATION_NOT_SET) {
      throw GrpcExceptions.fieldViolation("configuration", "a configuration case must be set");
    }

    accountsManager.update(AuthenticationUtil.requireAuthenticatedDevice().accountIdentifier(), account -> {
      account.setUnidentifiedAccessKey(request.hasUnidentifiedAccessKey()
          ? request.getUnidentifiedAccessKey().toByteArray()
          : null);
      account.setUnrestrictedUnidentifiedAccess(request.hasAllowUnrestrictedUnidentifiedAccess());
    });

    return ConfigureUnidentifiedAccessResponse.getDefaultInstance();
  }

  @Override
  public SetDiscoverableByPhoneNumberResponse setDiscoverableByPhoneNumber(final SetDiscoverableByPhoneNumberRequest request) {
    accountsManager.update(AuthenticationUtil.requireAuthenticatedDevice().accountIdentifier(),
        account -> account.setDiscoverableByPhoneNumber(request.getDiscoverableByPhoneNumber()));

    return SetDiscoverableByPhoneNumberResponse.getDefaultInstance();
  }

  @Override
  public SetRegistrationRecoveryPasswordResponse setRegistrationRecoveryPassword(final SetRegistrationRecoveryPasswordRequest request) {
    registrationRecoveryPasswordsManager.store(getAuthenticatedAccount().getIdentifier(IdentityType.PNI),
            request.getRegistrationRecoveryPassword().toByteArray());

    return SetRegistrationRecoveryPasswordResponse.getDefaultInstance();
  }

  @Override
  public SetZkCredentialKeyResponse setZkCredentialKey(final SetZkCredentialKeyRequest request) throws RateLimitExceededException {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    final Account authenticatedAccount = getAuthenticatedAccount();
    final ZkCredentialPublicKey zkCredentialKey;
    try {
      zkCredentialKey = new ZkCredentialPublicKey(request.getPublicKey().toByteArray());
    } catch (InvalidInputException _) {
      throw GrpcExceptions.invalidArguments("invalid public key bytes");
    }

    if (authenticatedAccount.getZkCredentialKey().map(zkCredentialKey::equals).orElse(false)) {
      return SetZkCredentialKeyResponse.newBuilder()
          .setRotationId(Objects.requireNonNull(authenticatedAccount.getZkCredentialKeyRotationId()))
          .build();
    }

    rateLimiters.getSetZkCredentialKeyLimiter().validate(authenticatedDevice.accountIdentifier());

    // It is technically fine from the credential's perspective if it is zero, but it's clearer to never have the default value
    final long rotationId = SECURE_RANDOM.nextLong(1, Long.MAX_VALUE);

    accountsManager.update(authenticatedDevice.accountIdentifier(), account -> {
      account.setZkCredentialKey(zkCredentialKey);
      account.setZkCredentialKeyRotationId(rotationId);
    });

    return SetZkCredentialKeyResponse.newBuilder()
        .setRotationId(rotationId)
        .build();
  }

  @Override
  public ChangeNumberResponse changeNumber(final ChangeNumberRequest request)
      throws RateLimitExceededException, InterruptedException {

    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedPrimaryDevice();

    final IdentityKey pniIdentityKey;
    try {
      pniIdentityKey = new IdentityKey(request.getPniIdentityKey().toByteArray());
    } catch (final InvalidKeyException e) {
      throw GrpcExceptions.fieldViolation("pni_identity_key", "invalid identity key");
    }

    final Map<Byte, ECSignedPreKey> devicePniSignedPreKeys = transformDeviceMap(request.getDevicePniSignedPreKeysMap(),
        key -> KeysGrpcHelper.checkEcSignedPreKey(key, pniIdentityKey,
            invalidPublicKeyException("device_pni_signed_pre_keys"),
            invalidSignatureException("device_pni_signed_pre_keys")));

    final Map<Byte, KEMSignedPreKey> devicePniPqLastResortPreKeys = transformDeviceMap(request.getDevicePniPqLastResortPreKeysMap(),
        key -> KeysGrpcHelper.checkKemSignedPreKey(key, pniIdentityKey,
            invalidPublicKeyException("device_pni_pq_last_resort_pre_keys"),
            invalidSignatureException("device_pni_pq_last_resort_pre_keys")));

    final Map<Byte, Integer> pniRegistrationIds = transformDeviceMap(request.getPniRegistrationIdsMap(), Function.identity());

    if (!pniRegistrationIds.values().stream().allMatch(RegistrationIdValidator::validRegistrationId)) {
      throw GrpcExceptions.fieldViolation("pni_registration_ids", "invalid registration id");
    }

    final List<IncomingMessage> deviceMessages = request.getDeviceMessages().getMessagesMap()
        .entrySet()
        .stream()
        .map(entry -> new IncomingMessage(
            getEnvelopeType(entry.getValue().getType()).getNumber(),
            (byte) (int) entry.getKey(),
            entry.getValue().getRegistrationId(),
            entry.getValue().getPayload().toByteArray()))
        .toList();

    final byte[] sessionId = request.getVerificationCase() == ChangeNumberRequest.VerificationCase.SESSION_ID
        ? request.getSessionId().toByteArray()
        : null;

    final byte[] recoveryPassword =
        request.getVerificationCase() == ChangeNumberRequest.VerificationCase.RECOVERY_PASSWORD
            ? request.getRecoveryPassword().toByteArray()
            : null;

    final String registrationLock = request.getRegistrationLock().isEmpty() ? null :
        formatRegistrationLock(request.getRegistrationLock().toByteArray());

    try {
      final Account updatedAccount = changeNumberManager.changeNumber(
          authenticatedDevice.accountIdentifier(),
          sessionId,
          recoveryPassword,
          registrationLock,
          request.getNumber(),
          pniIdentityKey,
          devicePniSignedPreKeys,
          devicePniPqLastResortPreKeys,
          deviceMessages,
          pniRegistrationIds,
          RequestAttributesUtil.getUserAgent().orElse(null),
          RequestAttributesUtil.getAcceptLanguageRaw(),
          RequestAttributesUtil.getRemoteAddress().getHostAddress());

      return ChangeNumberResponse.newBuilder()
          .setAccountIdentifiers(buildAccountIdentifiers(updatedAccount))
          .build();
    } catch (final MismatchedDevicesException e) {
      if (!e.getMismatchedDevices().staleDeviceIds().isEmpty()) {
        return ChangeNumberResponse.newBuilder()
            .setStaleDevices(StaleDevices.newBuilder()
                .addAllStaleDevices(e.getMismatchedDevices().staleDeviceIds().stream().map(Byte::intValue).toList()))
            .build();
      }

      return ChangeNumberResponse.newBuilder()
          .setMismatchedDevices(MessagesGrpcHelper.buildMismatchedDevices(
              new AciServiceIdentifier(authenticatedDevice.accountIdentifier()), e.getMismatchedDevices()))
          .build();
    } catch (final MessageTooLargeException e) {
      return ChangeNumberResponse.newBuilder()
          .setMessageTooLarge(FailedPrecondition.newBuilder()
              .setDescription("one or more device messages was too large"))
          .build();
    } catch (final MessageDeliveryNotAllowedException e) {
      throw GrpcExceptions.unavailable();
    } catch (final IllegalArgumentException e) {
      throw GrpcExceptions.invalidArguments(e.getMessage());
    } catch (final RegistrationLockFailureException e) {
      final RegistrationLockFailure.Builder failureBuilder = RegistrationLockFailure.newBuilder()
          .setTimeRemainingMillis(e.getFailure().timeRemaining());

      if (e.getFailure().svr2Credentials() != null) {
        failureBuilder.setSvr2Credentials(ExternalServiceCredentials.newBuilder()
            .setUsername(e.getFailure().svr2Credentials().username())
            .setPassword(e.getFailure().svr2Credentials().password()));
      }

      return ChangeNumberResponse.newBuilder()
          .setRegistrationLockFailure(failureBuilder)
          .build();
    } catch (final UnverifiedRegistrationSessionException e) {
      return ChangeNumberResponse.newBuilder()
          .setUnverifiedRegistrationSession(FailedPrecondition.getDefaultInstance())
          .build();
    } catch (final InvalidRegistrationSessionException e) {
      return ChangeNumberResponse.newBuilder()
          .setInvalidRegistrationSession(FailedPrecondition.newBuilder().setDescription(e.getMessage()))
          .build();
    } catch (final IOException e) {
      throw GrpcExceptions.unavailable(e.getMessage());
    } catch (final RecoveryPasswordVerificationFailedException e) {
      return ChangeNumberResponse.newBuilder()
          .setRecoveryPasswordVerificationFailed(FailedPrecondition.getDefaultInstance())
          .build();
    }
  }

  @Override
  public GetAccountDataReportResponse getAccountDataReport(final GetAccountDataReportRequest request)
      throws IOException {
    final Account account = getAuthenticatedAccount();

    final AccountDataReportResponse report = new AccountDataReportResponse(UUID.randomUUID(), clock.instant(),
        new AccountDataReportResponse.AccountAndDevicesDataReport(
            new AccountDataReportResponse.AccountDataReport(
                account.getNumber(),
                account.getBadges().stream().map(AccountDataReportResponse.BadgeDataReport::new).toList(),
                account.isUnrestrictedUnidentifiedAccess(),
                account.isDiscoverableByPhoneNumber()),
            account.getDevices().stream().map(device ->
                new AccountDataReportResponse.DeviceDataReport(
                    device.getId(),
                    Instant.ofEpochMilli(device.getLastSeen()),
                    Instant.ofEpochMilli(device.getCreated()),
                    device.getUserAgent())).toList()));

    return GetAccountDataReportResponse.newBuilder()
        .setJson(SystemMapper.jsonMapper().writeValueAsString(report))
        .setText(report.text())
        .build();
  }

  @Override
  public GetCapabilitiesResponse getCapabilities(GetCapabilitiesRequest request) {
    final Account account = getAuthenticatedAccount();
    final Capabilities.Builder builder = Capabilities.newBuilder();
    for (DeviceCapability capability : DeviceCapability.SELF_VISIBLE_CAPABILITIES) {
      if (account.hasCapability(capability)) {
        builder.addCapabilities(DeviceCapabilityUtil.toGrpcDeviceCapability(capability));
      }
    }
    return GetCapabilitiesResponse.newBuilder().setCapabilities(builder.build()).build();
  }

  private static AccountIdentifiers buildAccountIdentifiers(final Account account) {
    final AccountIdentifiers.Builder accountIdentifiersBuilder = AccountIdentifiers.newBuilder()
        .addServiceIdentifiers(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(account.getUuid())))
        .addServiceIdentifiers(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new PniServiceIdentifier(account.getPhoneNumberIdentifier())))
        .setE164(account.getNumber());

    account.getUsernameHash().ifPresent(usernameHash ->
        accountIdentifiersBuilder.setUsernameHash(ByteString.copyFrom(usernameHash)));

    if (account.getUsernameLinkHandle() != null) {
      accountIdentifiersBuilder.setUsernameLinkHandle(UUIDUtil.toByteString(account.getUsernameLinkHandle()));
    }

    return accountIdentifiersBuilder.build();
  }

  private static MessageProtos.Envelope.Type getEnvelopeType(final SendMessageType type) {
    return switch (type) {
      case DOUBLE_RATCHET -> MessageProtos.Envelope.Type.CIPHERTEXT;
      case PREKEY_MESSAGE -> MessageProtos.Envelope.Type.PREKEY_BUNDLE;
      case PLAINTEXT_CONTENT -> MessageProtos.Envelope.Type.PLAINTEXT_CONTENT;
      case UNIDENTIFIED_SENDER -> throw GrpcExceptions.invalidArguments("illegal envelope type for change-number sync messages");
      case UNSPECIFIED, UNRECOGNIZED -> throw GrpcExceptions.invalidArguments("unrecognized envelope type");
    };
  }

  private Account getAuthenticatedAccount() {
    return getAuthenticatedAccount(AuthenticationUtil.requireAuthenticatedDevice());
  }

  private Account getAuthenticatedAccount(final AuthenticatedDevice authenticatedDevice) {
    return accountsManager.getByAccountIdentifier(authenticatedDevice.accountIdentifier())
        .orElseThrow(() -> GrpcExceptions.invalidCredentials("invalid credentials"));
  }

  private static String formatRegistrationLock(final byte[] registrationLock) {
    // In the previous REST-based API, clients would send hex strings directly. For backward compatibility, we
    // convert the registration lock secret to a lowercase hex string before turning it into a salted hash.
    return HexFormat.of().withLowerCase().formatHex(registrationLock);
  }

  private static StatusRuntimeException invalidPublicKeyException(final String fieldName) {
    return GrpcExceptions.fieldViolation(fieldName, "invalid public key");
  }

  private static StatusRuntimeException invalidSignatureException(final String fieldName) {
    return  GrpcExceptions.fieldViolation(fieldName, "pre-key signature did not match PNI identity key");
  }

  private static <T, U> Map<Byte, U> transformDeviceMap(final Map<Integer, T> byDeviceId, final Function<T, U> f) {
    return byDeviceId.entrySet()
        .stream()
        .collect(Collectors.<Map.Entry<Integer, T>, Byte, U>toMap(entry -> entry.getKey().byteValue(),
            entry -> f.apply(entry.getValue())));
  }
}
