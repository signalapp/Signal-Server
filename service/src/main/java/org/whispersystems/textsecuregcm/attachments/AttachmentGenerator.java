/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.attachments;
import java.util.Map;

public interface AttachmentGenerator {

  record Descriptor(Map<String, String> headers, String signedUploadLocation) {}

  Descriptor generateAttachment(final String key);

}
