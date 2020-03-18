package org.whispersystems.textsecuregcm.gcp;

import io.dropwizard.util.Strings;

import javax.annotation.Nonnull;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

public class CanonicalRequestGenerator {
  private static final DateTimeFormatter SIMPLE_UTC_DATE = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.US).withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter SIMPLE_UTC_DATE_TIME = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'", Locale.US).withZone(ZoneOffset.UTC);

  @Nonnull
  private final String domain;

  @Nonnull
  private final String email;

  private final int maxSizeBytes;

  @Nonnull
  private final String pathPrefix;

  public CanonicalRequestGenerator(@Nonnull String domain, @Nonnull String email, int maxSizeBytes, @Nonnull String pathPrefix) {
    this.domain       = domain;
    this.email        = email;
    this.maxSizeBytes = maxSizeBytes;
    this.pathPrefix   = pathPrefix;
  }

  public CanonicalRequest createFor(@Nonnull final String key, @Nonnull final ZonedDateTime now) {
    final StringBuilder result = new StringBuilder("POST\n");

    final StringBuilder resourcePathBuilder = new StringBuilder();
    if (!Strings.isNullOrEmpty(pathPrefix)) {
      resourcePathBuilder.append(pathPrefix);
    }
    resourcePathBuilder.append('/').append(URLEncoder.encode(key, StandardCharsets.UTF_8));
    final String resourcePath = resourcePathBuilder.toString();
    result.append(resourcePath).append('\n');

    final String activeDatetime = SIMPLE_UTC_DATE_TIME.format(now);
    final String canonicalQuery = "X-Goog-Algorithm=GOOG4-RSA-SHA256" +
            "&X-Goog-Credential=" + URLEncoder.encode(makeCredential(email, now), StandardCharsets.UTF_8) +
            "&X-Goog-Date=" + URLEncoder.encode(activeDatetime, StandardCharsets.UTF_8) +
            "&X-Goog-Expires=" + Duration.of(25, ChronoUnit.HOURS).toSeconds() +
            "&X-Goog-SignedHeaders=host%3Bx-goog-content-length-range%3Bx-goog-resumable";
    result.append(canonicalQuery).append('\n');

    result.append("host:").append(domain).append('\n');
    result.append("x-goog-content-length-range:1,").append(maxSizeBytes).append('\n');
    result.append("x-goog-resumable:start\n");
    result.append('\n');

    result.append("host;x-goog-content-length-range;x-goog-resumable\n");

    result.append("UNSIGNED-PAYLOAD");

    return new CanonicalRequest(result.toString(), resourcePath, canonicalQuery, activeDatetime, makeCredentialScope(now), domain, maxSizeBytes);
  }

  private String makeCredentialScope(@Nonnull ZonedDateTime now) {
    return SIMPLE_UTC_DATE.format(now) + "/auto/storage/goog4_request";
  }

  private String makeCredential(@Nonnull String email, @Nonnull ZonedDateTime now) {
    return email + '/' + makeCredentialScope(now);
  }
}
