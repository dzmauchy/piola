package org.dauch.piola.sctp;

/*-
 * #%L
 * piola-server
 * %%
 * Copyright (C) 2024 dauch
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-3.0.html>.
 * #L%
 */

import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpMultiChannel;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static com.sun.nio.sctp.SctpStandardSocketOptions.*;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.TRACE;
import static org.dauch.piola.net.NetworkUtils.isLoopback;

public interface SctpUtils {

  static void configure(SctpMultiChannel channel, CommonConfig conf) throws IOException {
    channel.setOption(SCTP_DISABLE_FRAGMENTS, false, null);
    channel.setOption(SO_RCVBUF, conf.rcvBufSize(), null);
    channel.setOption(SO_SNDBUF, conf.sendBufSize(), null);
    channel.setOption(SCTP_NODELAY, conf.nagle(), null);
    channel.setOption(SCTP_FRAGMENT_INTERLEAVE, 0, null);
    channel.setOption(SO_LINGER, conf.linger(), null);
    channel.setOption(SCTP_INIT_MAXSTREAMS, InitMaxStreams.create(conf.maxStreams(), conf.maxStreams()), null);
    channel.configureBlocking(true);
  }

  static InetSocketAddress sendExitSequence(SctpMultiChannel ch) throws IOException {
    for (var addr : ch.getAllLocalAddresses()) {
      if (addr instanceof InetSocketAddress a && isLoopback(a)) {
        ch.send(ByteBuffer.allocate(1), MessageInfo.createOutgoing(a, 0).payloadProtocolID(Integer.MIN_VALUE));
        return a;
      }
    }
    throw new NoRouteToHostException("No loopback address found");
  }

  static void checkIncomplete(MessageInfo msg, MessageInfo m, Consumer<MessageInfo> closer) throws Exception {
    try {
      if (m.streamNumber() != msg.streamNumber())
        throw new StreamCorruptedException("Stream number mismatch: " + m);
      if (m.payloadProtocolID() != msg.payloadProtocolID())
        throw new StreamCorruptedException("Payload protocol ID mismatch: " + m);
      if (m.association().associationID() != msg.association().associationID())
        throw new StreamCorruptedException("Association mismatch: " + m);
    } catch (StreamCorruptedException e) {
      closer.accept(msg);
      closer.accept(m);
      throw e;
    }
  }

  static void closeAssociation(SctpMultiChannel channel, MessageInfo messageInfo, System.Logger logger) {
    try {
      channel.shutdown(messageInfo.association());
    } catch (IllegalArgumentException _) {
      logger.log(TRACE, () -> "Already closed " + messageInfo.association());
    } catch (Throwable e) {
      logger.log(ERROR, () -> "Unable to close the association " + messageInfo.association(), e);
    }
  }
}
