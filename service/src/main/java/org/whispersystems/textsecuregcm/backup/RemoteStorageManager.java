package org.whispersystems.textsecuregcm.backup;

import java.net.URI;
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
   * @param sourceUri            The location of the object to copy
   * @param expectedSourceLength The length of the source object, should match the content-length of the object returned
   *                             from the sourceUri.
   * @param encryptionParameters The encryption keys that should be used to apply an additional layer of encryption to
   *                             the object
   * @param uploadDescriptor     The destination, which must be in the cdn returned by {@link #cdnNumber()}
   * @return A stage that completes successfully when the source has been successfully re-encrypted and copied into
   * uploadDescriptor. The returned CompletionStage can be completed exceptionally with the following exceptions.
   * <ul>
   *  <li> {@link InvalidLengthException} If the expectedSourceLength does not match the length of the sourceUri </li>
   *  <li> {@link SourceObjectNotFoundException} If the no object at sourceUri is found </li>
   *  <li> {@link java.io.IOException} If there was a generic IO issue </li>
   * </ul>
   */
  CompletionStage<Void> copy(
      URI sourceUri,
      int expectedSourceLength,
      MediaEncryptionParameters encryptionParameters,
      MessageBackupUploadDescriptor uploadDescriptor);
}
