/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.entities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.util.Util;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.Mac;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class EncryptedOutgoingMessage {

  private final Logger logger = LoggerFactory.getLogger(EncryptedOutgoingMessage.class);

  private static final byte[] VERSION         = new byte[]{0x01};
  private static final int    CIPHER_KEY_SIZE = 32;
  private static final int    MAC_KEY_SIZE    = 20;
  private static final int    MAC_SIZE        = 10;

  private final byte[] serialized;
  private final String serializedAndEncoded;

  public EncryptedOutgoingMessage(Envelope outgoingMessage, String signalingKey)
      throws CryptoEncodingException
  {
    byte[]        plaintext  = outgoingMessage.toByteArray();
    SecretKeySpec cipherKey  = getCipherKey (signalingKey);
    SecretKeySpec macKey     = getMacKey(signalingKey);

    this.serialized           = getCiphertext(plaintext, cipherKey, macKey);
    this.serializedAndEncoded = Base64.encodeBytes(this.serialized);
  }

  public String toEncodedString() {
    return serializedAndEncoded;
  }

  public byte[] toByteArray() {
    return serialized;
  }

  private byte[] getCiphertext(byte[] plaintext, SecretKeySpec cipherKey, SecretKeySpec macKey)
      throws CryptoEncodingException
  {
    try {
      Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
      cipher.init(Cipher.ENCRYPT_MODE, cipherKey);

      Mac hmac = Mac.getInstance("HmacSHA256");
      hmac.init(macKey);

      hmac.update(VERSION);

      byte[] ivBytes = cipher.getIV();
      hmac.update(ivBytes);

      byte[] ciphertext   = cipher.doFinal(plaintext);
      byte[] mac          = hmac.doFinal(ciphertext);
      byte[] truncatedMac = new byte[MAC_SIZE];
      System.arraycopy(mac, 0, truncatedMac, 0, truncatedMac.length);

      return Util.combine(VERSION, ivBytes, ciphertext, truncatedMac);
    } catch (NoSuchAlgorithmException | NoSuchPaddingException | IllegalBlockSizeException | BadPaddingException e) {
      throw new AssertionError(e);
    } catch (InvalidKeyException e) {
      logger.warn("Invalid Key", e);
      throw new CryptoEncodingException("Invalid key!");
    }
  }

  private SecretKeySpec getCipherKey(String signalingKey) throws CryptoEncodingException {
    try {
      byte[] signalingKeyBytes = Base64.decode(signalingKey);
      byte[] cipherKey         = new byte[CIPHER_KEY_SIZE];

      if (signalingKeyBytes.length < CIPHER_KEY_SIZE)
        throw new CryptoEncodingException("Signaling key too short!");

      System.arraycopy(signalingKeyBytes, 0, cipherKey, 0, cipherKey.length);
      return new SecretKeySpec(cipherKey, "AES");
    } catch (IOException e) {
      throw new CryptoEncodingException(e);
    }
  }

  private SecretKeySpec getMacKey(String signalingKey) throws CryptoEncodingException {
    try {
      byte[] signalingKeyBytes = Base64.decode(signalingKey);
      byte[] macKey            = new byte[MAC_KEY_SIZE];

      if (signalingKeyBytes.length < CIPHER_KEY_SIZE + MAC_KEY_SIZE)
        throw new CryptoEncodingException("Signaling key too short!");

      System.arraycopy(signalingKeyBytes, CIPHER_KEY_SIZE, macKey, 0, macKey.length);

      return new SecretKeySpec(macKey, "HmacSHA256");
    } catch (IOException e) {
      throw new CryptoEncodingException(e);
    }
  }

}
