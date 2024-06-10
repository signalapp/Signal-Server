/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that an endpoint may change the "enabled" state of one or more devices associated with an account, and that
 * any websockets associated with the account may need to be refreshed after a call to that endpoint.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ChangesLinkedDevices {
}
