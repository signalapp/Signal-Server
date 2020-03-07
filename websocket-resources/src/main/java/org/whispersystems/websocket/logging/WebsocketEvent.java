package org.whispersystems.websocket.logging;

import com.google.common.annotations.VisibleForTesting;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;

import javax.ws.rs.core.MultivaluedMap;

import java.util.List;

import ch.qos.logback.core.spi.DeferredProcessingAware;

public class WebsocketEvent implements DeferredProcessingAware {

  public static final int    SENTINEL = -1;
  public static final String NA       = "-";

  private final String            remoteAddress;
  private final ContainerRequest  request;
  private final ContainerResponse response;
  private final long              timestamp;

  public WebsocketEvent(String remoteAddress, ContainerRequest jerseyRequest, ContainerResponse jettyResponse) {
    this.timestamp     = System.currentTimeMillis();
    this.remoteAddress = remoteAddress;
    this.request       = jerseyRequest;
    this.response      = jettyResponse;
  }

  public String getRemoteHost() {
    return remoteAddress;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void prepareForDeferredProcessing() {

  }

  public String getMethod() {
    return request.getMethod();
  }

  public String getPath() {
    return request.getBaseUri().getPath() + request.getPath(false);
  }

  public String getProtocol() {
    return "WS";
  }

  public int getStatusCode() {
    return response.getStatus();
  }

  public long getContentLength() {
    return response.getLength();
  }

  public String getRequestHeader(String key) {
    List<String> values = request.getRequestHeader(key);

    if (values == null) return NA;
    else                return values.stream().findFirst().orElse(NA);
  }

  public MultivaluedMap<String, String> getRequestHeaderMap() {
    return request.getRequestHeaders();
  }
}
