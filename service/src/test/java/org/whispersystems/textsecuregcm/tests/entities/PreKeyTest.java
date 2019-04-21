package org.whispersystems.textsecuregcm.tests.entities;

import org.junit.Test;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.util.Util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.*;

public class PreKeyTest {

  @Test
  public void deserializeFromJSONV() throws Exception {
    ClientContact contact = new ClientContact(Util.getContactToken("+14152222222"),
                                              "whisper", false, false);

    assertThat("a ClientContact can be deserialized from JSON",
               fromJson(jsonFixture("fixtures/contact.relay.json"), ClientContact.class),
               is(contact));
  }

  @Test
  public void serializeToJSONV2() throws Exception {
    PreKey preKey = new PreKey(1234, "test");

    assertThat("PreKeyV2 Serialization works",
               asJson(preKey),
               is(equalTo(jsonFixture("fixtures/prekey_v2.json"))));
  }

}
