/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.vdurmont.semver4j.Semver;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class DynamicRemoteDeprecationConfiguration {

    @JsonProperty
    private Map<ClientPlatform, Semver> minimumVersions = Collections.emptyMap();

    @JsonProperty
    private Map<ClientPlatform, Semver> versionsPendingDeprecation = Collections.emptyMap();

    @JsonProperty
    private Map<ClientPlatform, Set<Semver>> blockedVersions = Collections.emptyMap();

    @JsonProperty
    private Map<ClientPlatform, Set<Semver>> versionsPendingBlock = Collections.emptyMap();

    @JsonProperty
    private boolean unrecognizedUserAgentAllowed = true;

    @VisibleForTesting
    public void setMinimumVersions(final Map<ClientPlatform, Semver> minimumVersions) {
        this.minimumVersions = minimumVersions;
    }

    public Map<ClientPlatform, Semver> getMinimumVersions() {
        return minimumVersions;
    }

    @VisibleForTesting
    public void setVersionsPendingDeprecation(final Map<ClientPlatform, Semver> versionsPendingDeprecation) {
        this.versionsPendingDeprecation = versionsPendingDeprecation;
    }

    public Map<ClientPlatform, Semver> getVersionsPendingDeprecation() {
        return versionsPendingDeprecation;
    }

    @VisibleForTesting
    public void setUnrecognizedUserAgentAllowed(final boolean allowUnrecognizedUserAgents) {
        this.unrecognizedUserAgentAllowed = allowUnrecognizedUserAgents;
    }

    public boolean isUnrecognizedUserAgentAllowed() {
        return unrecognizedUserAgentAllowed;
    }

    @VisibleForTesting
    public void setBlockedVersions(final Map<ClientPlatform, Set<Semver>> blockedVersions) {
        this.blockedVersions = blockedVersions;
    }

    public Map<ClientPlatform, Set<Semver>> getBlockedVersions() {
        return blockedVersions;
    }

    @VisibleForTesting
    public void setVersionsPendingBlock(final Map<ClientPlatform, Set<Semver>> versionsPendingBlock) {
        this.versionsPendingBlock = versionsPendingBlock;
    }

    public Map<ClientPlatform, Set<Semver>> getVersionsPendingBlock() {
        return versionsPendingBlock;
    }
}
