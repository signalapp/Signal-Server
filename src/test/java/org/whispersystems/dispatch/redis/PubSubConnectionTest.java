package org.whispersystems.dispatch.redis;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

public class PubSubConnectionTest {

  private static final String REPLY = "*3\r\n" +
      "$9\r\n" +
      "subscribe\r\n" +
      "$5\r\n" +
      "abcde\r\n" +
      ":1\r\n" +
      "*3\r\n" +
      "$9\r\n" +
      "subscribe\r\n" +
      "$5\r\n" +
      "fghij\r\n" +
      ":2\r\n" +
      "*3\r\n" +
      "$9\r\n" +
      "subscribe\r\n" +
      "$5\r\n" +
      "klmno\r\n" +
      ":2\r\n" +
      "*3\r\n" +
      "$7\r\n" +
      "message\r\n" +
      "$5\r\n" +
      "abcde\r\n" +
      "$10\r\n" +
      "1234567890\r\n" +
      "*3\r\n" +
      "$7\r\n" +
      "message\r\n" +
      "$5\r\n" +
      "klmno\r\n" +
      "$10\r\n" +
      "0987654321\r\n";


  @Test
  public void testSubscribe() throws IOException {
//    ByteChannel      byteChannel = mock(ByteChannel.class);
    OutputStream outputStream = mock(OutputStream.class);
    Socket       socket       = mock(Socket.class      );
    when(socket.getOutputStream()).thenReturn(outputStream);
    PubSubConnection connection  = new PubSubConnection(socket);

    connection.subscribe("foobar");

    ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
    verify(outputStream).write(captor.capture());

    assertArrayEquals(captor.getValue(), "SUBSCRIBE foobar\r\n".getBytes());
  }

  @Test
  public void testUnsubscribe() throws IOException {
    OutputStream outputStream = mock(OutputStream.class);
    Socket       socket       = mock(Socket.class      );
    when(socket.getOutputStream()).thenReturn(outputStream);
    PubSubConnection connection  = new PubSubConnection(socket);

    connection.unsubscribe("bazbar");

    ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
    verify(outputStream).write(captor.capture());

    assertArrayEquals(captor.getValue(), "UNSUBSCRIBE bazbar\r\n".getBytes());
  }

  @Test
  public void testTricklyResponse() throws Exception {
    InputStream  inputStream  = mockInputStreamFor(new TrickleInputStream(REPLY.getBytes()));
    OutputStream outputStream = mock(OutputStream.class);
    Socket       socket       = mock(Socket.class      );
    when(socket.getOutputStream()).thenReturn(outputStream);
    when(socket.getInputStream()).thenReturn(inputStream);

    PubSubConnection pubSubConnection = new PubSubConnection(socket);
    readResponses(pubSubConnection);
  }

  @Test
  public void testFullResponse() throws Exception {
    InputStream  inputStream  = mockInputStreamFor(new FullInputStream(REPLY.getBytes()));
    OutputStream outputStream = mock(OutputStream.class);
    Socket       socket       = mock(Socket.class      );
    when(socket.getOutputStream()).thenReturn(outputStream);
    when(socket.getInputStream()).thenReturn(inputStream);

    PubSubConnection pubSubConnection = new PubSubConnection(socket);
    readResponses(pubSubConnection);
  }

  @Test
  public void testRandomLengthResponse() throws Exception {
    InputStream  inputStream  = mockInputStreamFor(new RandomInputStream(REPLY.getBytes()));
    OutputStream outputStream = mock(OutputStream.class);
    Socket       socket       = mock(Socket.class      );
    when(socket.getOutputStream()).thenReturn(outputStream);
    when(socket.getInputStream()).thenReturn(inputStream);

    PubSubConnection pubSubConnection = new PubSubConnection(socket);
    readResponses(pubSubConnection);
  }

  private InputStream mockInputStreamFor(final MockInputStream stub) throws IOException {
    InputStream result = mock(InputStream.class);

    when(result.read()).thenAnswer(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
        return stub.read();
      }
    });

    when(result.read(any(byte[].class))).thenAnswer(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
        byte[] buffer = (byte[])invocationOnMock.getArguments()[0];
        return stub.read(buffer, 0, buffer.length);
      }
    });

    when(result.read(any(byte[].class), anyInt(), anyInt())).thenAnswer(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
        byte[] buffer = (byte[]) invocationOnMock.getArguments()[0];
        int offset = (int) invocationOnMock.getArguments()[1];
        int length = (int) invocationOnMock.getArguments()[2];

        return stub.read(buffer, offset, length);
      }
    });

    return result;
  }

  private void readResponses(PubSubConnection pubSubConnection) throws Exception {
    PubSubReply reply = pubSubConnection.read();

    assertEquals(reply.getType(), PubSubReply.Type.SUBSCRIBE);
    assertEquals(reply.getChannel(), "abcde");
    assertFalse(reply.getContent().isPresent());

    reply = pubSubConnection.read();

    assertEquals(reply.getType(), PubSubReply.Type.SUBSCRIBE);
    assertEquals(reply.getChannel(), "fghij");
    assertFalse(reply.getContent().isPresent());

    reply = pubSubConnection.read();

    assertEquals(reply.getType(), PubSubReply.Type.SUBSCRIBE);
    assertEquals(reply.getChannel(), "klmno");
    assertFalse(reply.getContent().isPresent());

    reply = pubSubConnection.read();

    assertEquals(reply.getType(), PubSubReply.Type.MESSAGE);
    assertEquals(reply.getChannel(), "abcde");
    assertArrayEquals(reply.getContent().get(), "1234567890".getBytes());

    reply = pubSubConnection.read();

    assertEquals(reply.getType(), PubSubReply.Type.MESSAGE);
    assertEquals(reply.getChannel(), "klmno");
    assertArrayEquals(reply.getContent().get(), "0987654321".getBytes());
  }

  private interface MockInputStream {
    public int read();
    public int read(byte[] input, int offset, int length);
  }

  private static class TrickleInputStream implements MockInputStream {

    private final byte[] data;
    private int index = 0;

    private TrickleInputStream(byte[] data) {
      this.data = data;
    }

    public int read() {
      return data[index++];
    }

    public int read(byte[] input, int offset, int length) {
      input[offset] = data[index++];
      return 1;
    }

  }

  private static class FullInputStream implements MockInputStream {

    private final byte[] data;
    private int index = 0;

    private FullInputStream(byte[] data) {
      this.data = data;
    }

    public int read() {
      return data[index++];
    }

    public int read(byte[] input, int offset, int length) {
      int amount = Math.min(data.length - index, length);
      System.arraycopy(data, index, input, offset, amount);
      index += length;

      return amount;
    }
  }

  private static class RandomInputStream implements MockInputStream {
    private final byte[] data;
    private int index = 0;

    private RandomInputStream(byte[] data) {
      this.data = data;
    }

    public int read() {
      return data[index++];
    }

    public int read(byte[] input, int offset, int length) {
      int maxCopy    = Math.min(data.length - index, length);
      int randomCopy = new SecureRandom().nextInt(maxCopy) + 1;
      int copyAmount = Math.min(maxCopy, randomCopy);

      System.arraycopy(data, index, input, offset, copyAmount);
      index += copyAmount;

      return copyAmount;
    }

  }

}
