/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.fasterxml.jackson.annotation.JsonTypeName;
import jakarta.validation.constraints.NotBlank;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

@JsonTypeName("default")
public record FoundationDbClusterConfiguration(@NotBlank String clusterFileUrl) implements FoundationDbDatabaseFactory {

  @Override
  public Database build(final FDB fdb) throws IOException {
    final URI clusterFileUri = URI.create(clusterFileUrl());

    final File clusterFile = switch (clusterFileUri.getScheme()) {
      case "file" -> new File(clusterFileUri);

      case "http", "https" -> {
        try (final HttpClient clusterFileClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build()) {

          final HttpResponse<String> response = clusterFileClient.send(HttpRequest.newBuilder()
              .uri(URI.create(clusterFileUrl()))
              .timeout(Duration.ofSeconds(10))
              .GET()
              .build(), HttpResponse.BodyHandlers.ofString());

          if (response.statusCode() != 200) {
            throw new IOException("Could not load cluster file (status " + response.statusCode() + ")");
          }

          final File tempClusterFile = File.createTempFile("fdb.cluster-", "");
          tempClusterFile.deleteOnExit();

          try (final FileWriter fileWriter = new FileWriter(tempClusterFile)) {
            fileWriter.write(response.body());
          }

          yield tempClusterFile;
        } catch (final InterruptedException e) {
          throw new IOException("Interrupted while waiting for cluster file response", e);
        }
      }

      default -> throw new IllegalArgumentException("Unrecognized cluster file URI scheme: " + clusterFileUri.getScheme());
    };

    return fdb.open(clusterFile.getAbsolutePath());
  }
}
