/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.core.cli.Command;
import io.dropwizard.core.setup.Bootstrap;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class CopyToS3Command extends Command {

  private static final String SOURCE_URI_ARGUMENT = "sourceUri";
  private static final String DESTINATION_BUCKET_ARGUMENT = "destinationBucket";
  private static final String DESTINATION_FILENAME_ARGUMENT = "destinationFilename";
  private static final String MINIMUM_EXPECTED_SIZE_ARGUMENT = "minimumExpectedSize";

  private static final Logger logger = LoggerFactory.getLogger(CopyToS3Command.class);

  public CopyToS3Command() {
    super("copy-to-s3", "Copies a file from an HTTP source to an S3 bucket");
  }

  @Override
  public void configure(final Subparser subparser) {
    subparser.addArgument("--source")
        .dest(SOURCE_URI_ARGUMENT)
        .type(String.class)
        .required(true)
        .help("The source URI to copy");

    subparser.addArgument("--destination-bucket")
        .dest(DESTINATION_BUCKET_ARGUMENT)
        .type(String.class)
        .required(true)
        .help("The destination S3 bucket");

    subparser.addArgument("--destination-filename")
        .dest(DESTINATION_FILENAME_ARGUMENT)
        .type(String.class)
        .required(true)
        .help("The name of the destination file within the S3 bucket");

    subparser.addArgument("--minimum-size")
        .dest(MINIMUM_EXPECTED_SIZE_ARGUMENT)
        .type(Integer.class)
        .required(true)
        .help("The minimum size of the source file in bytes; smaller files will not be copied");
  }

  @Override
  public void run(final Bootstrap<?> bootstrap, final Namespace namespace) throws IOException, InterruptedException {
    final URI sourceUri = URI.create(namespace.getString(SOURCE_URI_ARGUMENT));
    final String destinationBucket = namespace.getString(DESTINATION_BUCKET_ARGUMENT);
    final String destinationFilename = namespace.getString(DESTINATION_FILENAME_ARGUMENT);
    final int minimumExpectedSizeInBytes = namespace.getInt(MINIMUM_EXPECTED_SIZE_ARGUMENT);

    copyToS3(sourceUri, destinationBucket, destinationFilename, minimumExpectedSizeInBytes);
  }

  public static void copyToS3(final URI sourceUri,
      final String s3Bucket,
      final String filename,
      final int minimumExpectedSize) throws IOException, InterruptedException {

    final HttpRequest httpRequest = HttpRequest.newBuilder()
        .uri(sourceUri)
        .timeout(Duration.ofMinutes(1))
        .GET()
        .build();

    try (final HttpClient httpClient = HttpClient.newBuilder()
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();
        final S3Client s3Client = S3Client.builder().build()) {

      final HttpResponse<byte[]> httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());

      if (httpResponse.statusCode() == 200) {
        if (httpResponse.body().length < minimumExpectedSize) {
          throw new IOException("Response body was below minimum: %d, %d"
              .formatted(httpResponse.body().length, minimumExpectedSize));
        }

        final String contentType = httpResponse.headers().firstValue("Content-Type").orElse("application/octet-stream");

        s3Client.putObject(PutObjectRequest.builder()
            .bucket(s3Bucket)
            .key(filename)
            .contentType(contentType)
            .contentLength((long) httpResponse.body().length)
            .build(), RequestBody.fromBytes(httpResponse.body()));

        logger.info("Copied {} bytes from {} to s3://{}/{}",
            httpResponse.body().length,
            httpRequest.uri(),
            s3Bucket,
            filename);
      } else {
        throw new IOException("Got a non-200 reply from source URI: " + httpResponse.statusCode());
      }
    }
  }
}
