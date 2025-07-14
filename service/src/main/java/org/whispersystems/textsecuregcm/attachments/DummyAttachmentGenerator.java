/*
 * Copyright 2023 Signal Messenger, LLC
 * Copyright 2025 Molly Instant Messenger
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.attachments;

public class DummyAttachmentGenerator implements AttachmentGenerator {
  public DummyAttachmentGenerator() {}

  @Override
  public Descriptor generateAttachment(final String key) {
    return null;
  }
}
