/*
 * Copyright (C) 2015 Open Whisper Systems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.gcm.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.gcm.server.internal.GcmResponseEntity;
import org.whispersystems.gcm.server.internal.GcmResponseListEntity;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import io.github.resilience4j.retry.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;

/**
 * The main interface to sending GCM messages.  Thread safe.
 *
 * @author Moxie Marlinspike
 */
public class Sender {

  private static final String PRODUCTION_URL = "https://fcm.googleapis.com/fcm/send";

  private final String                   authorizationHeader;
  private final URI                      uri;
  private final Retry                    retry;
  private final ObjectMapper             mapper;
  private final ScheduledExecutorService executorService;

  private final HttpClient[] clients = new HttpClient[10];
  private final SecureRandom random  = new SecureRandom();

  /**
   * Construct a Sender instance.
   *
   * @param apiKey Your application's GCM API key.
   */
  public Sender(String apiKey, ObjectMapper mapper) {
    this(apiKey, mapper, 10);
  }

  /**
   * Construct a Sender instance with a specified retry count.
   *
   * @param apiKey Your application's GCM API key.
   * @param retryCount The number of retries to attempt on a network error or 500 response.
   */
  public Sender(String apiKey, ObjectMapper mapper, int retryCount) {
    this(apiKey, mapper, retryCount, PRODUCTION_URL);
  }

  @VisibleForTesting
  public Sender(String apiKey, ObjectMapper mapper, int retryCount, String url) {
    this.mapper              = mapper;
    this.executorService     = Executors.newSingleThreadScheduledExecutor();
    this.uri                 = URI.create(url);
    this.authorizationHeader = String.format("key=%s", apiKey);
    this.retry               = Retry.of("fcm-sender", RetryConfig.custom()
                                                                       .maxAttempts(retryCount)
                                                                       .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(Duration.ofMillis(100), 2.0))
                                                                       .retryOnException(this::isRetryableException)
                                                                       .build());

    for (int i=0;i<clients.length;i++) {
      this.clients[i] = HttpClient.newBuilder()
                                  .version(HttpClient.Version.HTTP_2)
                                  .connectTimeout(Duration.ofSeconds(10))
                                  .build();
    }
  }

  private boolean isRetryableException(Throwable throwable) {
    while (throwable instanceof  CompletionException) {
      throwable = throwable.getCause();
    }

    return throwable instanceof ServerFailedException ||
           throwable instanceof  TimeoutException     ||
           throwable instanceof  IOException;
  }

  /**
   * Asynchronously send a message.
   *
   * @param message The message to send.
   * @return A future.
   */
  public CompletableFuture<Result> send(Message message) {
    try {
      HttpRequest request = HttpRequest.newBuilder()
                                       .uri(uri)
                                       .header("Authorization", authorizationHeader)
                                       .header("Content-Type", "application/json")
                                       .POST(HttpRequest.BodyPublishers.ofString(message.serialize()))
                                       .timeout(Duration.ofSeconds(10))
                                       .build();

      return retry.executeCompletionStage(executorService,
                                          () -> getClient().sendAsync(request, BodyHandlers.ofByteArray())
                                                      .thenApply(response -> {
                                                        switch (response.statusCode()) {
                                                          case 400: throw new CompletionException(new InvalidRequestException());
                                                          case 401: throw new CompletionException(new AuthenticationFailedException());
                                                          case 204:
                                                          case 200: return response.body();
                                                          default:  throw new CompletionException(new ServerFailedException("Bad status: " + response.statusCode()));
                                                        }
                                                      })
                                                      .thenApply(responseBytes -> {
                                                        try {
                                                          List<GcmResponseEntity> responseList = mapper.readValue(responseBytes, GcmResponseListEntity.class).getResults();

                                                          if (responseList == null || responseList.size() == 0) {
                                                            throw new CompletionException(new IOException("Empty response list!"));
                                                          }

                                                          GcmResponseEntity responseEntity = responseList.get(0);

                                                          return new Result(responseEntity.getCanonicalRegistrationId(),
                                                                            responseEntity.getMessageId(),
                                                                            responseEntity.getError());
                                                        } catch (IOException e) {
                                                          throw new CompletionException(e);
                                                        }
                                                      })).toCompletableFuture();
    } catch (JsonProcessingException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  public Retry getRetry() {
    return retry;
  }

  private HttpClient getClient() {
    return clients[random.nextInt(clients.length)];
  }

}