/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

class PhoneNumberIdentifiersTest {

  @RegisterExtension
  static DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.PNI);

  private PhoneNumberIdentifiers phoneNumberIdentifiers;

  @BeforeEach
  void setUp() {
    phoneNumberIdentifiers = new PhoneNumberIdentifiers(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        Tables.PNI.tableName());
  }

  @Test
  void getPhoneNumberIdentifier() {
    final String number = "+18005551234";
    final String differentNumber = "+18005556789";

    final UUID firstPni = phoneNumberIdentifiers.getPhoneNumberIdentifier(number).join();
    final UUID secondPni = phoneNumberIdentifiers.getPhoneNumberIdentifier(number).join();

    assertEquals(firstPni, secondPni);
    assertNotEquals(firstPni, phoneNumberIdentifiers.getPhoneNumberIdentifier(differentNumber).join());
  }

  @Test
  void generatePhoneNumberIdentifier() {
    final List<String> numbers = List.of("+18005551234", "+18005556789");
    // Should set both PNIs to a new random PNI
    final UUID pni = phoneNumberIdentifiers.setPniIfRequired(numbers.getFirst(), numbers, Collections.emptyMap()).join();

    assertEquals(pni, phoneNumberIdentifiers.getPhoneNumberIdentifier(numbers.getFirst()).join());
    assertEquals(pni, phoneNumberIdentifiers.getPhoneNumberIdentifier(numbers.getLast()).join());
  }

  @Test
  void generatePhoneNumberIdentifierOneFormExists() {
    final String firstNumber = "+18005551234";
    final String secondNumber = "+18005556789";
    final String thirdNumber = "+1800555456";
    final List<String> allNumbers = List.of(firstNumber, secondNumber, thirdNumber);

    // Set one member of the "same" numbers to a new PNI
    final UUID pni = phoneNumberIdentifiers.getPhoneNumberIdentifier(secondNumber).join();

    final Map<String, UUID> existingAssociations = phoneNumberIdentifiers.fetchPhoneNumbers(allNumbers).join();
    assertEquals(Map.of(secondNumber, pni), existingAssociations);

    assertEquals(pni, phoneNumberIdentifiers.setPniIfRequired(firstNumber, allNumbers, existingAssociations).join());

    for (String number : allNumbers) {
      assertEquals(pni, phoneNumberIdentifiers.getPhoneNumberIdentifier(number).join());
    }
  }

  @Test
  void getPhoneNumberIdentifierExistingMapping() {
    final String newFormatBeninE164 = PhoneNumberUtil.getInstance()
        .format(PhoneNumberUtil.getInstance().getExampleNumber("BJ"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final String oldFormatBeninE164 = newFormatBeninE164.replaceFirst("01", "");
    final UUID oldFormatPni = phoneNumberIdentifiers.getPhoneNumberIdentifier(oldFormatBeninE164).join();
    final UUID newFormatPni = phoneNumberIdentifiers.getPhoneNumberIdentifier(newFormatBeninE164).join();
    assertEquals(oldFormatPni, newFormatPni);
  }

  @Test
  void conflictingExistingPnis() {
    final String firstNumber = "+18005551234";
    final String secondNumber = "+18005556789";

    final UUID firstPni = phoneNumberIdentifiers.getPhoneNumberIdentifier(firstNumber).join();
    final UUID secondPni = phoneNumberIdentifiers.getPhoneNumberIdentifier(secondNumber).join();
    assertNotEquals(firstPni, secondPni);

    assertEquals(
        firstPni,
        phoneNumberIdentifiers.setPniIfRequired(
            firstNumber, List.of(firstNumber, secondNumber),
            phoneNumberIdentifiers.fetchPhoneNumbers(List.of(firstNumber, secondNumber)).join()).join());
    assertEquals(
        secondPni,
        phoneNumberIdentifiers.setPniIfRequired(
            secondNumber, List.of(secondNumber, firstNumber),
            phoneNumberIdentifiers.fetchPhoneNumbers(List.of(firstNumber, secondNumber)).join()).join());
  }

  @Test
  void conflictOnOriginalNumber() {
    final List<String> numbers = List.of("+18005551234", "+18005556789");
    // Stale view of database where both numbers have no PNI
    final Map<String, UUID> existingAssociations = Collections.emptyMap();

    // Both numbers have different PNIs
    final UUID pni1 = phoneNumberIdentifiers.getPhoneNumberIdentifier(numbers.getFirst()).join();
    final UUID pni2 = phoneNumberIdentifiers.getPhoneNumberIdentifier(numbers.getLast()).join();
    assertNotEquals(pni1, pni2);

    // Should conflict and find that we now have a PNI
    assertEquals(pni1, phoneNumberIdentifiers.setPniIfRequired(numbers.getFirst(), numbers, existingAssociations).join());
  }

  @Test
  void conflictOnAlternateNumber() {
    final List<String> numbers = List.of("+18005551234", "+18005556789");
    // Stale view of database where both numbers have no PNI
    final Map<String, UUID> existingAssociations = Collections.emptyMap();

    // the alternate number has a PNI added
    phoneNumberIdentifiers.getPhoneNumberIdentifier(numbers.getLast()).join();

    // Should conflict and fail
    CompletableFutureTestUtil.assertFailsWithCause(
        TransactionCanceledException.class,
        phoneNumberIdentifiers.setPniIfRequired(numbers.getFirst(), numbers, existingAssociations));
  }

  @Test
  void multipleAssociations() {
    final List<String> numbers = List.of("+18005550000", "+18005551111", "+18005552222", "+18005553333", "+1800555444");

    // Set pni1={number1, number2}, pni2={number3}, number0 and number 4 unset
    final UUID pni1 = phoneNumberIdentifiers.setPniIfRequired(numbers.get(1), numbers.subList(1, 3),
        Collections.emptyMap()).join();
    final UUID pni2 = phoneNumberIdentifiers.setPniIfRequired(numbers.get(3), List.of(numbers.get(3)),
        Collections.emptyMap()).join();

    final Map<String, UUID> existingAssociations = phoneNumberIdentifiers.fetchPhoneNumbers(numbers).join();
    assertEquals(existingAssociations, Map.of(numbers.get(1), pni1, numbers.get(2), pni1, numbers.get(3), pni2));

    // The unmapped phone numbers should map to the arbitrarily selected PNI (which is selected based on the order
    // of the numbers)
    assertEquals(pni1, phoneNumberIdentifiers.setPniIfRequired(numbers.get(0), numbers, existingAssociations).join());
    assertEquals(pni1, phoneNumberIdentifiers.getPhoneNumberIdentifier(numbers.get(0)).join());
    assertEquals(pni1, phoneNumberIdentifiers.getPhoneNumberIdentifier(numbers.get(4)).join());
  }

  private static class FailN implements Supplier<CompletableFuture<Integer>> {
    final AtomicInteger numFails;

    FailN(final int numFails) {
      this.numFails = new AtomicInteger(numFails);
    }

    @Override
    public CompletableFuture<Integer> get() {
      if (numFails.getAndDecrement() == 0) {
        return CompletableFuture.completedFuture(7);
      }
      return CompletableFuture.failedFuture(new IOException("test"));
    }
  }

  @Test
  void testRetry() {
    assertEquals(7, PhoneNumberIdentifiers.retry(10, IOException.class, new FailN(9)).join());

    CompletableFutureTestUtil.assertFailsWithCause(
        IOException.class,
        PhoneNumberIdentifiers.retry(10, IOException.class, new FailN(10)));

    CompletableFutureTestUtil.assertFailsWithCause(
        IOException.class,
        PhoneNumberIdentifiers.retry(10, RuntimeException.class, new FailN(1)));
  }

  @Test
  void getPhoneNumber() {
    final String number = "+18005551234";

    assertTrue(phoneNumberIdentifiers.getPhoneNumber(UUID.randomUUID()).join().isEmpty());

    final UUID pni = phoneNumberIdentifiers.getPhoneNumberIdentifier(number).join();
    assertEquals(List.of(number), phoneNumberIdentifiers.getPhoneNumber(pni).join());
  }

  @Test
  void regeneratePhoneNumberIdentifierMappings() {
    // libphonenumber 8.13.50 and on generate new-format numbers for Benin
    final String newFormatBeninE164 = PhoneNumberUtil.getInstance()
        .format(PhoneNumberUtil.getInstance().getExampleNumber("BJ"), PhoneNumberUtil.PhoneNumberFormat.E164);
    final String oldFormatBeninE164 = newFormatBeninE164.replaceFirst("01", "");

    final UUID phoneNumberIdentifier = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn(newFormatBeninE164);
    when(account.getIdentifier(IdentityType.PNI)).thenReturn(phoneNumberIdentifier);

    phoneNumberIdentifiers.regeneratePhoneNumberIdentifierMappings(account).join();

    assertEquals(phoneNumberIdentifier, phoneNumberIdentifiers.getPhoneNumberIdentifier(newFormatBeninE164).join());
    assertEquals(phoneNumberIdentifier, phoneNumberIdentifiers.getPhoneNumberIdentifier(oldFormatBeninE164).join());
    assertEquals(Set.of(newFormatBeninE164, oldFormatBeninE164),
        new HashSet<>(phoneNumberIdentifiers.getPhoneNumber(phoneNumberIdentifier).join()));
  }
}
