/**
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.nurkiewicz.asyncretry.AsyncRetryExecutor;
import com.nurkiewicz.asyncretry.RetryContext;
import com.nurkiewicz.asyncretry.RetryExecutor;
import com.nurkiewicz.asyncretry.function.RetryCallable;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.whispersystems.gcm.server.internal.GcmResponseEntity;
import org.whispersystems.gcm.server.internal.GcmResponseListEntity;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * The main interface to sending GCM messages.  Thread safe.
 *
 * @author Moxie Marlinspike
 */
public class Sender {

  private static final String PRODUCTION_URL = "https://fcm.googleapis.com/fcm/send";

  private final CloseableHttpAsyncClient client;
  private final String                   authorizationHeader;
  private final RetryExecutor            executor;
  private final String                   url;

  /**
   * Construct a Sender instance.
   *
   * @param apiKey Your application's GCM API key.
   */
  public Sender(String apiKey) {
    this(apiKey, 10);
  }

  /**
   * Construct a Sender instance with a specified retry count.
   *
   * @param apiKey Your application's GCM API key.
   * @param retryCount The number of retries to attempt on a network error or 500 response.
   */
  public Sender(String apiKey, int retryCount) {
    this(apiKey, retryCount, PRODUCTION_URL);
  }

  @VisibleForTesting
  public Sender(String apiKey, int retryCount, String url) {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    this.url                 = url;
    this.authorizationHeader = String.format("key=%s", apiKey);

    this.client = HttpAsyncClients.custom()
                                  .setMaxConnTotal(100)
                                  .setMaxConnPerRoute(10)
                                  .build();

    this.executor = new AsyncRetryExecutor(scheduler).retryOn(ServerFailedException.class)
                                                     .retryOn(TimeoutException.class)
                                                     .retryOn(IOException.class)
                                                     .withExponentialBackoff(100, 2.0)
                                                     .withUniformJitter()
                                                     .withMaxDelay(4000)
                                                     .withMaxRetries(retryCount);

    this.client.start();
  }

  /**
   * Asynchronously send a message.
   *
   * @param message The message to send.
   * @return A future.
   */
  public ListenableFuture<Result> send(Message message) {
    return send(message, null);
  }

  /**
   * Asynchronously send a message with a context to be passed in the future result.
   *
   * @param message The message to send.
   * @param requestContext An opaque context to include the future result.
   * @return The future.
   */
  public ListenableFuture<Result> send(final Message message, final Object requestContext) {
    return executor.getFutureWithRetry(new RetryCallable<ListenableFuture<Result>>() {
      @Override
      public ListenableFuture<Result> call(RetryContext context) throws Exception {
        SettableFuture<Result> future  = SettableFuture.create();
        HttpPost               request = new HttpPost(url);

        request.setHeader("Authorization", authorizationHeader);
        request.setEntity(new StringEntity(message.serialize(),
                                           ContentType.parse("application/json")));

        client.execute(request, new ResponseHandler(future, requestContext));

        return future;
      }
    });
  }

  /**
   * Shut down all existing HTTP connections.
   * @throws IOException
   */
  public void stop() throws IOException {
    this.client.close();
  }

  private static final class ResponseHandler implements FutureCallback<HttpResponse> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
      objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final SettableFuture<Result> future;
    private final Object                 requestContext;

    public ResponseHandler(SettableFuture<Result> future, Object requestContext) {
      this.future         = future;
      this.requestContext = requestContext;
    }

    @Override
    public void completed(HttpResponse result) {
      try {
        String responseBody = EntityUtils.toString(result.getEntity());

        switch (result.getStatusLine().getStatusCode()) {
          case 400: future.setException(new InvalidRequestException());       break;
          case 401: future.setException(new AuthenticationFailedException()); break;
          case 204:
          case 200: future.set(parseResult(responseBody));                    break;
          default:  future.setException(new ServerFailedException("Bad status: " + result.getStatusLine().getStatusCode()));
        }
      } catch (IOException e) {
        future.setException(e);
      }
    }

    @Override
    public void failed(Exception ex) {
      future.setException(ex);
    }

    @Override
    public void cancelled() {
      future.setException(new ServerFailedException("Canceled!"));
    }

    private Result parseResult(String body) throws IOException {
      List<GcmResponseEntity> responseList = objectMapper.readValue(body, GcmResponseListEntity.class)
                                                         .getResults();

      if (responseList == null || responseList.size() == 0) {
        throw new IOException("Empty response list!");
      }

      GcmResponseEntity responseEntity = responseList.get(0);

      return new Result(this.requestContext,
                        responseEntity.getCanonicalRegistrationId(),
                        responseEntity.getMessageId(),
                        responseEntity.getError());
    }
  }
}