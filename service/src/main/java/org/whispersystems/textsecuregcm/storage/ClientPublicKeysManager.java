package org.whispersystems.textsecuregcm.storage;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;

/**
 * A client public key manager provides access to clients' public keys for use in transport-level authentication and
 * encryption.
 */
public class ClientPublicKeysManager {

  private final ClientPublicKeys clientPublicKeys;

  private final AccountLockManager accountLockManager;
  private final Executor accountLockExecutor;

  public ClientPublicKeysManager(final ClientPublicKeys clientPublicKeys,
      final AccountLockManager accountLockManager,
      final Executor accountLockExecutor) {

    this.clientPublicKeys = clientPublicKeys;
    this.accountLockManager = accountLockManager;
    this.accountLockExecutor = accountLockExecutor;
  }

  /**
   * Stores the given public key for the given account/device, overwriting any previously-stored public key. This method
   * is intended for use for adding public keys to existing accounts/devices as a migration step. Callers should use
   * {@link #buildTransactWriteItemForInsertion(UUID, byte, ECPublicKey)} instead when creating new accounts/devices.
   *
   * @param account the target account
   * @param deviceId the identifier for the target device
   * @param publicKey the public key to store for the target account/device

   * @return a future that completes when the given key has been stored
   */
  public CompletableFuture<Void> setPublicKey(final Account account, final byte deviceId, final ECPublicKey publicKey) {
    return accountLockManager.withLockAsync(List.of(account.getPhoneNumberIdentifier()),
        () -> clientPublicKeys.setPublicKey(account.getIdentifier(IdentityType.ACI), deviceId, publicKey),
        accountLockExecutor);
  }

  /**
   * Builds a {@link TransactWriteItem} that will store a public key for the given account/device. Intended for use when
   * adding devices to accounts or creating new accounts.
   *
   * @param accountIdentifier the identifier for the target account
   * @param deviceId the identifier for the target device
   * @param publicKey the public key to store for the target account/device
   *
   * @return a {@code TransactWriteItem} that will store the given public key for the given account/device
   */
  public TransactWriteItem buildTransactWriteItemForInsertion(final UUID accountIdentifier,
      final byte deviceId,
      final ECPublicKey publicKey) {

    return clientPublicKeys.buildTransactWriteItemForInsertion(accountIdentifier, deviceId, publicKey);
  }

  /**
   * Builds a {@link TransactWriteItem} that will remove the public key for the given account/device. Intended for use
   * when removing devices from accounts or deleting/re-creating accounts.
   *
   * @param accountIdentifier the identifier for the target account
   * @param deviceId the identifier for the target device
   *
   * @return a {@code TransactWriteItem} that will remove the public key for the given account/device
   */
  public TransactWriteItem buildTransactWriteItemForDeletion(final UUID accountIdentifier, final byte deviceId) {
    return clientPublicKeys.buildTransactWriteItemForDeletion(accountIdentifier, deviceId);
  }

  /**
   * Finds the public key for the given account/device.
   *
   * @param accountIdentifier the identifier for the target account
   * @param deviceId the identifier for the target device
   *
   * @return a future that yields the Ed25519 public key for the given account/device, or empty if no public key was
   * found
   */
  public CompletableFuture<Optional<ECPublicKey>> findPublicKey(final UUID accountIdentifier, final byte deviceId) {
    return clientPublicKeys.findPublicKey(accountIdentifier, deviceId);
  }
}
