package org.whispersystems.textsecuregcm.currency;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.whispersystems.textsecuregcm.util.SystemMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class FtxClient {

  private final HttpClient client;

  public FtxClient(HttpClient client) {
    this.client = client;
  }

  public BigDecimal getSpotPrice(String currency, String base) throws FtxException{
    try {
      URI uri = URI.create("https://ftx.com/api/markets/" + currency + "/" + base);

      HttpResponse<String> response =  client.send(HttpRequest.newBuilder()
                                                              .GET()
                                                              .uri(uri)
                                                              .build(),
                                                   HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new FtxException("Bad response: " + response.statusCode() + " " + response.toString());
      }

      FtxResponse parsedResponse = SystemMapper.getMapper().readValue(response.body(), FtxResponse.class);

      return parsedResponse.result.price;

    } catch (IOException | InterruptedException e) {
      throw new FtxException(e);
    }
  }

  private static class FtxResponse {

    @JsonProperty
    private FtxResult result;

  }

  private static class FtxResult {

    @JsonProperty
    private BigDecimal price;

  }

  public static class FtxException extends IOException {
    public FtxException(String message) {
      super(message);
    }

    public FtxException(Exception exception) {
      super(exception);
    }
  }

}
