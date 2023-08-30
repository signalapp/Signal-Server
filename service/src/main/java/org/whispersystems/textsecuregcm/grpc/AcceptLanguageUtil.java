/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Context;
import java.util.List;
import java.util.Locale;

public class AcceptLanguageUtil {
  static final Context.Key<List<Locale>> ACCEPTABLE_LANGUAGES_CONTEXT_KEY = Context.key("accept-language");
  public static List<Locale> localeFromGrpcContext() {
    return ACCEPTABLE_LANGUAGES_CONTEXT_KEY.get();
  }
}
