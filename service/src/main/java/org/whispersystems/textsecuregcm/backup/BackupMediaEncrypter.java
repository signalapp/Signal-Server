package org.whispersystems.textsecuregcm.backup;

import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.Flow;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.Mac;
import javax.crypto.NoSuchPaddingException;
import org.reactivestreams.FlowAdapters;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class BackupMediaEncrypter {

  private final Cipher cipher;
  private final Mac mac;

  public BackupMediaEncrypter(final MediaEncryptionParameters encryptionParameters) {
    cipher = initializeCipher(encryptionParameters);
    mac = initializeMac(encryptionParameters);
  }

  public int outputSize(final int inputSize) {
    return cipher.getIV().length + cipher.getOutputSize(inputSize) + mac.getMacLength();
  }

  /**
   * Perform streaming encryption
   *
   * @param sourceBody A source of ByteBuffers, typically from an asynchronous HttpResponse
   * @return A publisher that returns IV + AES/CBC/PKCS5Padding encrypted source + HMAC(IV + encrypted source) suitable
   * to write with an asynchronous HttpRequest
   */
  public Flow.Publisher<ByteBuffer> encryptBody(Flow.Publisher<List<ByteBuffer>> sourceBody) {

    // Write IV, encrypted payload, mac
    final Flux<ByteBuffer> encryptedBody = Flux.concat(
        Mono.fromSupplier(() -> {
          mac.update(cipher.getIV());
          return ByteBuffer.wrap(cipher.getIV());
        }),
        Flux.from(FlowAdapters.toPublisher(sourceBody))
            .flatMap(buffers -> Flux.fromIterable(buffers))
            .concatMap(byteBuffer -> {
              final byte[] copy = new byte[byteBuffer.remaining()];
              byteBuffer.get(copy);
              final byte[] res = cipher.update(copy);
              if (res == null) {
                return Mono.empty();
              } else {
                mac.update(res);
                return Mono.just(ByteBuffer.wrap(res));
              }
            }),
        Mono.fromSupplier(() -> {
          try {
            final byte[] finalBytes = cipher.doFinal();
            mac.update(finalBytes);
            return ByteBuffer.wrap(finalBytes);
          } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw ExceptionUtils.wrap(e);
          }
        }),
        Mono.fromSupplier(() -> ByteBuffer.wrap(mac.doFinal())));
    return FlowAdapters.toFlowPublisher(encryptedBody);
  }

  private static Mac initializeMac(final MediaEncryptionParameters encryptionParameters) {
    try {
      final Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(encryptionParameters.hmacSHA256Key());
      return mac;
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    } catch (InvalidKeyException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static Cipher initializeCipher(final MediaEncryptionParameters encryptionParameters) {
    try {
      final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
      cipher.init(
          Cipher.ENCRYPT_MODE,
          encryptionParameters.aesEncryptionKey(),
          encryptionParameters.iv());
      return cipher;

    } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw new AssertionError(e);
    } catch (InvalidAlgorithmParameterException | InvalidKeyException e) {
      throw new IllegalArgumentException(e);
    }
  }

}
