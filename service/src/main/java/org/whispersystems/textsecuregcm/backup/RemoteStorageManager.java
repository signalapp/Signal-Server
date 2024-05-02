package org.whispersystems.textsecuregcm.backup;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Handles management operations over a external cdn storage system.
 */
public interface RemoteStorageManager {

  /**
   * @return The cdn number that this RemoteStorageManager manages
   */
  int cdnNumber();

  /**
   * Copy and the object from a remote source into the backup, adding an additional layer of encryption
   *
   * @param sourceCdn            The cdn number where the source attachment is stored
   * @param sourceKey            The key of the source attachment within the attachment cdn
   * @param expectedSourceLength The length of the source object, should match the content-length of the object returned
   *                             from the sourceUri.
   * @param encryptionParameters The encryption keys that should be used to apply an additional layer of encryption to
   *                             the object
   * @param dstKey               The key within the backup cdn where the copied object will be written
   * @return A stage that completes successfully when the source has been successfully re-encrypted and copied into
   * uploadDescriptor. The returned CompletionStage can be completed exceptionally with the following exceptions.
   * <ul>
   *  <li> {@link InvalidLengthException} If the expectedSourceLength does not match the length of the sourceUri </li>
   *  <li> {@link SourceObjectNotFoundException} If the no object at sourceUri is found </li>
   *  <li> {@link java.io.IOException} If there was a generic IO issue </li>
   * </ul>
   */
  CompletionStage<Void> copy(
      int sourceCdn,
      String sourceKey,
      int expectedSourceLength,
      MediaEncryptionParameters encryptionParameters,
      String dstKey);

  /**
   * Result of a {@link #list} operation
   *
   * @param objects An {@link Entry} for each object returned by the list request
   * @param cursor  An opaque string that can be used to resume listing from where a previous request left off, empty if
   *                the request reached the end of the list of matching objects.
   */
  record ListResult(List<Entry> objects, Optional<String> cursor) {

    /**
     * An entry representing a remote stored object under a prefix
     *
     * @param key    The name of the object with the prefix removed
     * @param length The length of the object in bytes
     */
    record Entry(String key, long length) {}
  }

  /**
   * List objects on the remote cdn.
   *
   * @param prefix The prefix of the objects to list
   * @param cursor The cursor returned by a previous call to list, or empty if starting from the first object with the
   *               provided prefix
   * @param limit  The maximum number of items to return in the list
   * @return A {@link ListResult} of objects that match the prefix.
   */
  CompletionStage<ListResult> list(final String prefix, final Optional<String> cursor, final long limit);

  /**
   * Calculate the total number of bytes stored by objects with the provided prefix
   *
   * @param prefix The prefix of the objects to sum
   * @return The number of bytes used
   */
  CompletionStage<UsageInfo> calculateBytesUsed(final String prefix);

  /**
   * Delete the specified object.
   *
   * @param key the key of the stored object to delete.
   * @return the number of bytes freed by the deletion operation
   */
  CompletionStage<Long> delete(final String key);
}
