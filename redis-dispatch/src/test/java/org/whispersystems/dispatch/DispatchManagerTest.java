/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.dispatch;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.whispersystems.dispatch.io.RedisPubSubConnectionFactory;
import org.whispersystems.dispatch.redis.PubSubConnection;
import org.whispersystems.dispatch.redis.PubSubReply;

public class DispatchManagerTest {

  private PubSubConnection             pubSubConnection;
  private RedisPubSubConnectionFactory socketFactory;
  private DispatchManager              dispatchManager;
  private PubSubReplyInputStream       pubSubReplyInputStream;

  @BeforeEach
  void setUp() throws Exception {
    pubSubConnection       = mock(PubSubConnection.class  );
    socketFactory          = mock(RedisPubSubConnectionFactory.class);
    pubSubReplyInputStream = new PubSubReplyInputStream();

    when(socketFactory.connect()).thenReturn(pubSubConnection);
    when(pubSubConnection.read()).thenAnswer((Answer<PubSubReply>) invocationOnMock -> pubSubReplyInputStream.read());

    dispatchManager = new DispatchManager(socketFactory, Optional.empty());
    dispatchManager.start();
  }

  @AfterEach
  void tearDown() {
    dispatchManager.shutdown();
  }

  @Test
  public void testConnect() {
    verify(socketFactory).connect();
  }

  @Test
  public void testSubscribe() {
    DispatchChannel dispatchChannel = mock(DispatchChannel.class);
    dispatchManager.subscribe("foo", dispatchChannel);
    pubSubReplyInputStream.write(new PubSubReply(PubSubReply.Type.SUBSCRIBE, "foo", Optional.empty()));

    verify(dispatchChannel, timeout(1000)).onDispatchSubscribed(eq("foo"));
  }

  @Test
  public void testSubscribeUnsubscribe() {
    DispatchChannel dispatchChannel = mock(DispatchChannel.class);
    dispatchManager.subscribe("foo", dispatchChannel);
    dispatchManager.unsubscribe("foo", dispatchChannel);

    pubSubReplyInputStream.write(new PubSubReply(PubSubReply.Type.SUBSCRIBE, "foo", Optional.empty()));
    pubSubReplyInputStream.write(new PubSubReply(PubSubReply.Type.UNSUBSCRIBE, "foo", Optional.empty()));

    verify(dispatchChannel, timeout(1000)).onDispatchUnsubscribed(eq("foo"));
  }

  @Test
  public void testMessages() {
    DispatchChannel fooChannel = mock(DispatchChannel.class);
    DispatchChannel barChannel = mock(DispatchChannel.class);

    dispatchManager.subscribe("foo", fooChannel);
    dispatchManager.subscribe("bar", barChannel);

    pubSubReplyInputStream.write(new PubSubReply(PubSubReply.Type.SUBSCRIBE, "foo", Optional.empty()));
    pubSubReplyInputStream.write(new PubSubReply(PubSubReply.Type.SUBSCRIBE, "bar", Optional.empty()));

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
