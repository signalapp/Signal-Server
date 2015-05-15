package org.whispersystems.textsecuregcm.tests.push;


import org.junit.Test;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager.ApnFallbackTask;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager.ApnFallbackTaskQueue;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApnFallbackTaskQueueTest {

  @Test
  public void testBlocking() {
    final ApnFallbackTaskQueue taskQueue = new ApnFallbackTaskQueue();

    final WebsocketAddress address = mock(WebsocketAddress.class);
    final ApnFallbackTask  task    = mock(ApnFallbackTask.class );

    when(task.getExecutionTime()).thenReturn(System.currentTimeMillis() - 1000);

    new Thread() {
      @Override
      public void run() {
        Util.sleep(500);
        taskQueue.put(address, task);
      }
    }.start();

    Map.Entry<WebsocketAddress, ApnFallbackTask> result = taskQueue.get();

    assertEquals(result.getKey(), address);
    assertEquals(result.getValue(), task);
  }

  @Test
  public void testElapsedTime() {
    final ApnFallbackTaskQueue taskQueue = new ApnFallbackTaskQueue();
    final WebsocketAddress     address   = mock(WebsocketAddress.class);
    final ApnFallbackTask      task      = mock(ApnFallbackTask.class );

    long currentTime = System.currentTimeMillis();

    when(task.getExecutionTime()).thenReturn(currentTime + 1000);

    taskQueue.put(address, task);
    Map.Entry<WebsocketAddress, ApnFallbackTask> result = taskQueue.get();

    assertTrue(System.currentTimeMillis() >= currentTime + 1000);
    assertEquals(result.getKey(), address);
    assertEquals(result.getValue(), task);
  }

  @Test
  public void testCanceled() {
    final ApnFallbackTaskQueue taskQueue = new ApnFallbackTaskQueue();
    final WebsocketAddress     addressOne   = mock(WebsocketAddress.class);
    final ApnFallbackTask      taskOne      = mock(ApnFallbackTask.class );
    final WebsocketAddress     addressTwo   = mock(WebsocketAddress.class);
    final ApnFallbackTask      taskTwo      = mock(ApnFallbackTask.class );

    long currentTime = System.currentTimeMillis();

    when(taskOne.getExecutionTime()).thenReturn(currentTime + 1000);
    when(taskTwo.getExecutionTime()).thenReturn(currentTime + 2000);

    taskQueue.put(addressOne, taskOne);
    taskQueue.put(addressTwo, taskTwo);

    new Thread() {
      @Override
      public void run() {
        Util.sleep(300);
        taskQueue.remove(addressOne);
      }
    }.start();

    Map.Entry<WebsocketAddress, ApnFallbackTask> result = taskQueue.get();

    assertTrue(System.currentTimeMillis() >= currentTime + 2000);
    assertEquals(result.getKey(), addressTwo);
    assertEquals(result.getValue(), taskTwo);
  }


}
