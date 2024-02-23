package org.whispersystems.textsecuregcm.grpc.net;

interface WebSocketCloseListener {

  WebSocketCloseListener NOOP_LISTENER = new WebSocketCloseListener() {
    @Override
    public void handleWebSocketClosedByClient(final int statusCode) {
    }

    @Override
    public void handleWebSocketClosedByServer(final int statusCode) {
    }
  };

  void handleWebSocketClosedByClient(int statusCode);

  void handleWebSocketClosedByServer(int statusCode);
}
