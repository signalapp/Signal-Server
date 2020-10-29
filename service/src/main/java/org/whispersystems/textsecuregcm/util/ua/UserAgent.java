/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.ua;

import com.vdurmont.semver4j.Semver;

import java.util.Objects;
import java.util.Optional;

public class UserAgent {

    private final ClientPlatform platform;
    private final Semver         version;
    private final String         additionalSpecifiers;

    public UserAgent(final ClientPlatform platform, final Semver version) {
        this(platform, version, null);
    }

    public UserAgent(final ClientPlatform platform, final Semver version, final String additionalSpecifiers) {
        this.platform = platform;
        this.version = version;
        this.additionalSpecifiers = additionalSpecifiers;
    }

    public ClientPlatform getPlatform() {
        return platform;
    }

    public Semver getVersion() {
        return version;
    }

    public Optional<String> getAdditionalSpecifiers() {
        return Optional.ofNullable(additionalSpecifiers);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final UserAgent userAgent = (UserAgent)o;
        return platform == userAgent.platform &&
                version.equals(userAgent.version) &&
                Objects.equals(additionalSpecifiers, userAgent.additionalSpecifiers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(platform, version, additionalSpecifiers);
    }

    @Override
    public String toString() {
        return "UserAgent{" +
                "platform=" + platform +
                ", version=" + version +
                ", additionalSpecifiers='" + additionalSpecifiers + '\'' +
                '}';
    }
}
