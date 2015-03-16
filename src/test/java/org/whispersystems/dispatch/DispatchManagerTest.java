package org.whispersystems.dispatch;

import com.google.common.base.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.whispersystems.dispatch.io.RedisPubSubConnectionFactory;
import org.whispersystems.dispatch.redis.PubSubConnection;
import org.whispersystems.dispatch.redis.PubSubReply;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class DispatchManagerTest {

  private PubSubConnection             pubSubConnection;
  private RedisPubSubConnectionFactory socketFactory;
  private DispatchManager              dispatchManager;
  private PubSubReplyInputStream       pubSubReplyInputStream;

  @Rule
  public ExternalResource resource = new ExternalResource() {
    @Override
    protected void before() throws Throwable {
      pubSubConnection       = mock(PubSubConnection.class  );
      socketFactory          = mock(RedisPubSubConnectionFactory.class);
      pubSubReplyInputStream = new PubSubReplyInputStream();

      when(socketFactory.connect()).thenReturn(pubSubConnection);
      when(pubSubConnection.read()).thenAnswer(new Answer<PubSubReply>() {
        @Override
        public PubSubReply answer(InvocationOnMock invocationOnMock) throws Throwable {
          return pubSubReplyInputStream.read();
        }
      });

      dispatchManager = new DispatchManager(socketFactory, Optional.<DispatchChannel>absent());
      dispatchManager.start();
    }

    @Override
    protected void after() {

    }
  };

  @Test
  public void testConnect() {
    verify(socketFactory).connect();
  }

  @Test
  public void testSubscribe() throws IOException {
    DispatchChannel dispatchChannel = mock(DispatchChannel.class);
    dispatchManager.subscribe("foo", dispatchChannel);
    pubSubReplyInputStream.write(new PubSubReply(PubSubReply.Type.SUBSCRIBE, "foo", Optional.<byte[]>absent()));

    verify(dispatchChannel, timeout(1000)).onDispatchSubscribed(eq("foo"));
  }

  @Test
  public void testSubscribeUnsubscribe() throws IOException {
    DispatchChannel dispatchChannel = mock(DispatchChannel.class);
    dispatchManager.subscribe("foo", dispatchChannel);
    dispatchManager.unsubscribe("foo", dispatchChannel);

    pubSubReplyInputStream.write(new PubSubReply(PubSubReply.Type.SUBSCRIBE, "foo", Optional.<byte[]>absent()));
    pubSubReplyInputStream.write(new PubSubReply(PubSubReply.Type.UNSUBSCRIBE, "foo", Optional.<byte[]>absent()));

    verify(dispatchChannel, timeout(1000)).onDispatchUnsubscribed(eq("foo"));
  }

  @Test
  public void testMessages() throws IOException {
    DispatchChannel fooChannel = mock(DispatchChannel.class);
    DispatchChannel barChannel = mock(DispatchChannel.class);

    dispatchManager.subscribe("foo", fooChannel);
    dispatchManager.subscribe("bar", barChannel);

    pubSubReplyInputStream.write(new PubSubReply(PubSubReply.Type.SUBSCRIBE, "foo", Optional.<byte[]>absent()));
    pubSubReplyInputStream.write(new PubSubReply(PubSubReply.Type.SUBSCRIBE, "bar", Optional.<byte[]>absent()));

    verify(fooChannel, timeout(1000)).onDispatchSubscribed(eq("foo"));
    verify(barChannel, timeout(1000)).onDispatchSubscribed(eq("bar"));

    pubSubReplyInputStream.write(new PubSubReply(PubSubReply.Type.MESSAGE, "foo", Optional.of("hello".getBytes())));
    pubSubReplyInputStream.write(new PubSubReply(PubSubReply.Type.MESSAGE, "bar", Optional.of("there".getBytes())));

    ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
    verify(fooChannel, timeout(1000)).onDispatchMessage(eq("foo"), captor.capture());

    assertArrayEquals("hello".getBytes(), captor.getValue());

    verify(barChannel, timeout(1000)).onDispatchMessage(eq("bar"), captor.capture());

    assertArrayEquals("there".getBytes(), captor.getValue());
  }

  private static class PubSubReplyInputStream {

    private final List<PubSubReply> pubSubReplyList = new LinkedList<>();

    public synchronized PubSubReply read() {
      try {
        while (pubSubReplyList.isEmpty()) wait();
        return pubSubReplyList.remove(0);
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
    }

    public synchronized void write(PubSubReply pubSubReply) {
      pubSubReplyList.add(pubSubReply);
      notifyAll();
    }
  }

}
