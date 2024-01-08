package org.whispersystems.textsecuregcm.configuration;

public record Cdn3StorageManagerConfiguration(
    String baseUri,
    String clientId,
    String clientSecret) {}
