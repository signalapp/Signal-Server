/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import org.whispersystems.textsecuregcm.util.Nickname;
import javax.annotation.Nullable;
import javax.validation.Valid;

public record UsernameRequest(@Valid @Nickname String nickname, @Nullable String existingUsername) {}
