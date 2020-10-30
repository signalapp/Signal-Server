package org.whispersystems.textsecuregcm.controllers;

public class InvalidDestinationException extends Exception {
  public InvalidDestinationException(String message) {
    super(message);
  }
}
