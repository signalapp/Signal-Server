/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

public interface PreKey<K> {

  long keyId();

  K publicKey();

  byte[] serializedPublicKey();
}
