/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.registration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdToken;
import com.google.auth.oauth2.ImpersonatedCredentials;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;
import io.grpc.CallCredentials;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

public class IdentityTokenCallCredentialsTest {

  @Test
  public void retryErrors() throws IOException {
    final ImpersonatedCredentials impersonatedCredentials = mock(ImpersonatedCredentials.class);
    when(impersonatedCredentials.getSourceCredentials()).thenReturn(mock(GoogleCredentials.class));

    final IdentityTokenCallCredentials creds = new IdentityTokenCallCredentials(
        RetryConfig.custom()
            .retryOnException(throwable -> true)
            .maxAttempts(Integer.MAX_VALUE)
            .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(
                Duration.ofMillis(100), 1.5, Duration.ofSeconds(5)))
            .build(),
        impersonatedCredentials,
        "test",
        Executors.newSingleThreadScheduledExecutor());

    final IdToken idToken = mock(IdToken.class);
    when(idToken.getTokenValue()).thenReturn("testtoken");

    // throw exception first two calls, then succeed
    when(impersonatedCredentials.idTokenWithAudience(anyString(), any()))
        .thenThrow(new IOException("uh oh 1"))
        .thenThrow(new IOException("uh oh 2"))
        .thenReturn(idToken)
        .thenThrow(new IOException("uh oh 3"));

    creds.refreshIdentityToken();
    CallCredentials.MetadataApplier metadataApplier = mock(CallCredentials.MetadataApplier.class);
    creds.applyRequestMetadata(null, null, metadataApplier);
    verify(metadataApplier, times(1))
        .apply(argThat(metadata -> "Bearer testtoken".equals(metadata.get(IdentityTokenCallCredentials.AUTHORIZATION_METADATA_KEY))));
  }
}
