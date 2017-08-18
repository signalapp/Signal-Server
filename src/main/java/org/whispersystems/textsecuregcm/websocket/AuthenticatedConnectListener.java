package org.whispersystems.textsecuregcm.websocket;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PubSubManager;
import org.whispersystems.textsecuregcm.storage.PubSubProtos.PubSubMessage;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.websocket.session.WebSocketSessionContext;
import org.whispersystems.websocket.setup.WebSocketConnectListener;

import java.security.SecureRandom;

import static com.codahale.metrics.MetricRegistry.name;

public class AuthenticatedConnectListener implements WebSocketConnectListener {

  private static final Logger         logger         = LoggerFactory.getLogger(WebSocketConnection.class);
  private static final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          durationTimer  = metricRegistry.timer(name(WebSocketConnection.class, "connected_duration"));

  private final AccountsManager    accountsManager;
  private final PushSender         pushSender;
  private final ReceiptSender      receiptSender;
  private final MessagesManager    messagesManager;
  private final PubSubManager      pubSubManager;

  public AuthenticatedConnectListener(AccountsManager accountsManager, PushSender pushSender,
                                      ReceiptSender receiptSender,  MessagesManager messagesManager,
                                      PubSubManager pubSubManager)
  {
    this.accountsManager = accountsManager;
    this.pushSender      = pushSender;
    this.receiptSender   = receiptSender;
    this.messagesManager = messagesManager;
    this.pubSubManager   = pubSubManager;
  }

  @Override
  public void onWebSocketConnect(WebSocketSessionContext context) {
    final Account                 account        = context.getAuthenticated(Account.class);
    final Device                  device         = account.getAuthenticatedDevice().get();
    final String                  connectionId   = String.valueOf(new SecureRandom().nextLong());
    final Timer.Context           timer          = durationTimer.time();
    final WebsocketAddress        address        = new WebsocketAddress(account.getNumber(), device.getId());
    final WebSocketConnectionInfo info           = new WebSocketConnectionInfo(address);
    final WebSocketConnection     connection     = new WebSocketConnection(pushSender, receiptSender,
                                                                         messagesManager, account, device,
                                                                         context.getClient(), connectionId);
    final PubSubMessage           connectMessage = PubSubMessage.newBuilder().setType(PubSubMessage.Type.CONNECTED)
                                                                .setContent(ByteString.copyFrom(connectionId.getBytes()))
                                                                .build();

    pubSubManager.publish(info, connectMessage);
    pubSubManager.publish(address, connectMessage);
    pubSubManager.subscribe(address, connection);

    context.addListener(new WebSocketSessionContext.WebSocketEventListener() {
      @Override
      public void onWebSocketClose(WebSocketSessionContext context, int statusCode, String reason) {
        pubSubManager.unsubscribe(address, connection);
        timer.stop();
      }
    });
  }
}

