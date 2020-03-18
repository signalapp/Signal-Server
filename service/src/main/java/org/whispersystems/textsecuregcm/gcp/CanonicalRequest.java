package org.whispersystems.textsecuregcm.gcp;

import javax.annotation.Nonnull;

public class CanonicalRequest {

  @Nonnull
  private final String canonicalRequest;

  @Nonnull
  private final String resourcePath;

  @Nonnull
  private final String canonicalQuery;

  @Nonnull
  private final String activeDatetime;

  @Nonnull
  private final String credentialScope;

  @Nonnull
  private final String domain;

  private final int maxSizeInBytes;

  public CanonicalRequest(@Nonnull String canonicalRequest, @Nonnull String resourcePath, @Nonnull String canonicalQuery, @Nonnull String activeDatetime, @Nonnull String credentialScope, @Nonnull String domain, int maxSizeInBytes) {
    this.canonicalRequest = canonicalRequest;
    this.resourcePath     = resourcePath;
    this.canonicalQuery   = canonicalQuery;
    this.activeDatetime   = activeDatetime;
    this.credentialScope  = credentialScope;
    this.domain           = domain;
    this.maxSizeInBytes   = maxSizeInBytes;
  }

  @Nonnull
  String getCanonicalRequest() {
    return canonicalRequest;
  }

  @Nonnull
  public String getResourcePath() {
    return resourcePath;
  }

  @Nonnull
  public String getCanonicalQuery() {
    return canonicalQuery;
  }

  @Nonnull
  String getActiveDatetime() {
    return activeDatetime;
  }

  @Nonnull
  String getCredentialScope() {
    return credentialScope;
  }

  @Nonnull
  public String getDomain() {
    return domain;
  }

  public int getMaxSizeInBytes() {
    return maxSizeInBytes;
  }
}
