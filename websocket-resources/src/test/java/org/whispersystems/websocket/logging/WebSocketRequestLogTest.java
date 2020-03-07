package org.whispersystems.websocket.logging;

import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.junit.Test;
import org.whispersystems.websocket.WebSocketSecurityContext;
import org.whispersystems.websocket.session.ContextPrincipal;
import org.whispersystems.websocket.session.WebSocketSessionContext;

import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.OutputStreamAppender;
import ch.qos.logback.core.spi.DeferredProcessingAware;
import io.dropwizard.logging.AbstractOutputStreamAppenderFactory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class WebSocketRequestLogTest {

  @Test
  public void testLogLineWithoutHeaders() throws InterruptedException {
    WebSocketSessionContext sessionContext = mock(WebSocketSessionContext.class);

    ListAppender<WebsocketEvent>  listAppender         = new ListAppender<>();
    WebsocketRequestLoggerFactory requestLoggerFactory = new WebsocketRequestLoggerFactory();
    requestLoggerFactory.appenders = List.of(new ListAppenderFactory<>(listAppender));

    WebsocketRequestLog requestLog = requestLoggerFactory.build("test-logger");
    ContainerRequest    request    = new ContainerRequest (null, URI.create("/v1/test"), "GET", new WebSocketSecurityContext(new ContextPrincipal(sessionContext)), new MapPropertiesDelegate(new HashMap<>()), null);
    ContainerResponse   response   = new ContainerResponse(request, Response.ok("My response body").build());

    requestLog.log("123.456.789.123", request, response);

    listAppender.waitForListSize(1);
    assertThat(listAppender.list.size()).isEqualTo(1);

    String loggedLine = new String(listAppender.outputStream.toByteArray());
    assertThat(loggedLine.matches("123\\.456\\.789\\.123 \\- \\- \\[[0-9]{2}\\/[a-zA-Z]{3}\\/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} \\-[0-9]{4}\\] \"GET \\/v1\\/test WS\" 200 \\- \"\\-\" \"\\-\"\n")).isTrue();
  }

  @Test
  public void testLogLineWithHeaders() throws InterruptedException {
    WebSocketSessionContext sessionContext = mock(WebSocketSessionContext.class);

    ListAppender<WebsocketEvent>  listAppender         = new ListAppender<>();
    WebsocketRequestLoggerFactory requestLoggerFactory = new WebsocketRequestLoggerFactory();
    requestLoggerFactory.appenders = List.of(new ListAppenderFactory<>(listAppender));

    WebsocketRequestLog requestLog = requestLoggerFactory.build("test-logger");
    ContainerRequest    request    = new ContainerRequest (null, URI.create("/v1/test"), "GET", new WebSocketSecurityContext(new ContextPrincipal(sessionContext)), new MapPropertiesDelegate(new HashMap<>()), null);
    request.header("User-Agent", "SmertZeSmert");
    request.header("Referer", "https://moxie.org");
    ContainerResponse   response   = new ContainerResponse(request, Response.ok("My response body").build());

    requestLog.log("123.456.789.123", request, response);

    listAppender.waitForListSize(1);
    assertThat(listAppender.list.size()).isEqualTo(1);

    String loggedLine = new String(listAppender.outputStream.toByteArray());
    assertThat(loggedLine.matches("123\\.456\\.789\\.123 \\- \\- \\[[0-9]{2}\\/[a-zA-Z]{3}\\/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} \\-[0-9]{4}\\] \"GET \\/v1\\/test WS\" 200 \\- \"https://moxie.org\" \"SmertZeSmert\"\n")).isTrue();

    System.out.println(listAppender.list.get(0));
    System.out.println(new String(listAppender.outputStream.toByteArray()));
  }


  private static class ListAppenderFactory<T extends DeferredProcessingAware> extends AbstractOutputStreamAppenderFactory<T> {
    private final ListAppender<T> listAppender;

    public ListAppenderFactory(ListAppender<T> listAppender) {
      this.listAppender = listAppender;
    }

    @Override
    protected OutputStreamAppender<T> appender(LoggerContext context) {
      listAppender.setContext(context);
      return listAppender;
    }
  }

  private static class ListAppender<E> extends OutputStreamAppender<E> {

    public  final List<E>               list         = new ArrayList<E>();
    public  final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    protected void append(E e) {
      super.append(e);

      synchronized (list) {
        list.add(e);
        list.notifyAll();
      }
    }

    @Override
    public void start() {
      setOutputStream(outputStream);
      super.start();
    }

    public void waitForListSize(int size) throws InterruptedException {
      synchronized (list) {
        while (list.size() < size) {
          list.wait(5000);
        }
      }
    }

  }


}
