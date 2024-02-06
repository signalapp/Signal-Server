/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.auth;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An @{@link io.dropwizard.auth.Auth} object annotated with {@link ReadOnly} indicates that the consumer of the object
 * will never modify the object, nor its underlying canonical source.
 * <p>
 * For example, a consumer of a @ReadOnly AuthenticatedAccount promises to never modify the in-memory
 * AuthenticatedAccount and to never modify the underlying Account database for the account.
 *
 * @see org.whispersystems.websocket.ReusableAuth
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.PARAMETER})
public @interface ReadOnly {
}

