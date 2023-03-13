/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum Action {
  CHALLENGE("challenge"),
  REGISTRATION("registration");

  private final String actionName;

  Action(String actionName) {
    this.actionName = actionName;
  }

  public String getActionName() {
    return actionName;
  }

  private static final Map<String, Action> ENUM_MAP = Arrays
      .stream(Action.values())
      .collect(Collectors.toMap(
          a -> a.actionName,
          Function.identity()));
  @JsonCreator
  public static Action fromString(String key) {
    return ENUM_MAP.get(key.toLowerCase(Locale.ROOT).strip());
  }

  static Optional<Action> parse(final String action) {
    return Optional.ofNullable(fromString(action));
  }
}
