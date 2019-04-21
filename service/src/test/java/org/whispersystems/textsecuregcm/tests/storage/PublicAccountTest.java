package org.whispersystems.textsecuregcm.tests.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.PublicAccount;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class PublicAccountTest {

  @Test
  public void testPinSanitation() throws IOException {
    Set<Device>   devices       = Collections.singleton(new Device(1, "foo", "bar", "12345", null, "gcm-1234", null, null, true, 1234, new SignedPreKey(1, "public-foo", "signature-foo"), 31337, 31336, "Android4Life", true));
    Account       account       = new Account("+14151231234", devices, new byte[16]);
    account.setPin("123456");

    PublicAccount publicAccount = new PublicAccount(account);

    String   serialized = SystemMapper.getMapper().writeValueAsString(publicAccount);
    JsonNode result     = SystemMapper.getMapper().readTree(serialized);

    assertEquals("******", result.get("pin").textValue());
    assertNull(result.get("number"));
  }


}
