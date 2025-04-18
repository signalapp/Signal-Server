/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import java.net.InetAddress;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;

public record RequestAttributes(InetAddress remoteAddress,
                                @Nullable String userAgent,
                                List<Locale.LanguageRange> acceptLanguage) {
}
