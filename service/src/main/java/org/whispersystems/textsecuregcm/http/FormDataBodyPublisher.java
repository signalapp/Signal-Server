package org.whispersystems.textsecuregcm.http;

import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class FormDataBodyPublisher {

  public static HttpRequest.BodyPublisher of(Map<String, String> data) {
    StringBuilder builder = new StringBuilder();

    for (Map.Entry<String, String> entry : data.entrySet()) {
      if (builder.length() > 0) {
        builder.append("&");
      }

      builder.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));
      builder.append("=");
      builder.append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
    }

    return HttpRequest.BodyPublishers.ofString(builder.toString());
  }

}
