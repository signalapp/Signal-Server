package org.whispersystems.textsecuregcm.tests.redis;

import org.junit.Test;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

public class ReplicatedJedisPoolTest {

  @Test
  public void testWriteCheckoutNoSlaves() {
    JedisPool master = mock(JedisPool.class);

    try {
      new ReplicatedJedisPool(master, new LinkedList<>());
      throw new AssertionError();
    } catch (Exception e) {
      // good
    }
  }

  @Test
  public void testWriteCheckoutWithSlaves() {
    JedisPool master   = mock(JedisPool.class);
    JedisPool slave    = mock(JedisPool.class);
    Jedis     instance = mock(Jedis.class    );

    when(master.getResource()).thenReturn(instance);

    ReplicatedJedisPool replicatedJedisPool = new ReplicatedJedisPool(master, Collections.singletonList(slave));
    Jedis writeResource = replicatedJedisPool.getWriteResource();

    assertThat(writeResource).isEqualTo(instance);
    verify(master, times(1)).getResource();
  }

  @Test
  public void testReadCheckouts() {
    JedisPool master      = mock(JedisPool.class);
    JedisPool slaveOne    = mock(JedisPool.class);
    JedisPool slaveTwo    = mock(JedisPool.class);
    Jedis     instanceOne = mock(Jedis.class    );
    Jedis     instanceTwo = mock(Jedis.class    );

    when(slaveOne.getResource()).thenReturn(instanceOne);
    when(slaveTwo.getResource()).thenReturn(instanceTwo);

    ReplicatedJedisPool replicatedJedisPool = new ReplicatedJedisPool(master, Arrays.asList(slaveOne, slaveTwo));

    assertThat(replicatedJedisPool.getReadResource()).isEqualTo(instanceOne);
    assertThat(replicatedJedisPool.getReadResource()).isEqualTo(instanceTwo);
    assertThat(replicatedJedisPool.getReadResource()).isEqualTo(instanceOne);
    assertThat(replicatedJedisPool.getReadResource()).isEqualTo(instanceTwo);
    assertThat(replicatedJedisPool.getReadResource()).isEqualTo(instanceOne);

    verifyNoMoreInteractions(master);
  }

  @Test
  public void testBrokenReadCheckout() {
    JedisPool master      = mock(JedisPool.class);
    JedisPool slaveOne    = mock(JedisPool.class);
    JedisPool slaveTwo    = mock(JedisPool.class);
    Jedis     instanceTwo = mock(Jedis.class    );

    when(slaveOne.getResource()).thenThrow(new JedisException("Connection failed!"));
    when(slaveTwo.getResource()).thenReturn(instanceTwo);

    ReplicatedJedisPool replicatedJedisPool = new ReplicatedJedisPool(master, Arrays.asList(slaveOne, slaveTwo));

    assertThat(replicatedJedisPool.getReadResource()).isEqualTo(instanceTwo);
    assertThat(replicatedJedisPool.getReadResource()).isEqualTo(instanceTwo);
    assertThat(replicatedJedisPool.getReadResource()).isEqualTo(instanceTwo);

    verifyNoMoreInteractions(master);
  }

  @Test
  public void testAllBrokenReadCheckout() {
    JedisPool master      = mock(JedisPool.class);
    JedisPool slaveOne    = mock(JedisPool.class);
    JedisPool slaveTwo    = mock(JedisPool.class);

    when(slaveOne.getResource()).thenThrow(new JedisException("Connection failed!"));
    when(slaveTwo.getResource()).thenThrow(new JedisException("Also failed!"));

    ReplicatedJedisPool replicatedJedisPool = new ReplicatedJedisPool(master, Arrays.asList(slaveOne, slaveTwo));

    try {
      replicatedJedisPool.getReadResource();
      throw new AssertionError();
    } catch (Exception e) {
      // good
    }

    verifyNoMoreInteractions(master);
  }

}
