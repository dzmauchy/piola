package org.dauch.piola.test.listener;

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

import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.*;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static java.lang.Character.MAX_RADIX;
import static java.lang.System.Logger.Level.INFO;
import static java.nio.charset.StandardCharsets.UTF_8;
import static net.bytebuddy.matcher.ElementMatchers.named;

public final class ByteBufferTestExecutionListener implements TestExecutionListener {

  private final System.Logger logger = System.getLogger(ByteBufferTestExecutionListener.class.getName());

  @Override
  public void testPlanExecutionStarted(TestPlan testPlan) {
    ByteBuddyAgent.install();
    logger.log(INFO, "Byte buddy agent installed");

    try {
      var testClassesDir = Path.of(getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
      var targetDir = testClassesDir.getParent();
      var outFile = targetDir.resolve("buffers.txt");
      System.getProperties().put("piolaOut", new PrintStream(outFile.toFile(), UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    new AgentBuilder.Default()
      .type(named("org.dauch.piola.buffer.BufferManager"))
      .transform((b, _, _, _, _) -> b
        .method(named("get"))
        .intercept(Advice.to(BufferManagerGetAdvice.class))
        .method(named("release"))
        .intercept(Advice.to(BufferManagerReleaseAdvice.class))
      )
      .installOnByteBuddyAgent();
  }

  @Override
  public void testPlanExecutionFinished(TestPlan testPlan) {
    if (System.getProperties().get("piolaOut") instanceof PrintStream ps) {
      ps.close();
    }
  }

  public static final class BufferManagerGetAdvice {

    @OnMethodExit
    public static void get(@This Object manager, @Return ByteBuffer value) {
      if (System.getProperties().get("piolaOut") instanceof PrintStream ps) {
        try {
          var index = manager.getClass().getMethod("find", ByteBuffer.class).invoke(manager, value);
          ps.format("<- %s %s %s%n", index, manager, Thread.currentThread()).flush();
        } catch (ReflectiveOperationException e) {
          throw new IllegalStateException(e);
        }
      }
    }
  }

  public static final class BufferManagerReleaseAdvice {

    @OnMethodEnter
    public static void release(@This Object manager, @Argument(0) ByteBuffer value) {
      if (System.getProperties().get("piolaOut") instanceof PrintStream ps) {
        try {
          var index = manager.getClass().getMethod("find", ByteBuffer.class).invoke(manager, value);
          ps.format("-> %s %s %s%n", index, manager, Thread.currentThread()).flush();
        } catch (ReflectiveOperationException e) {
          throw new IllegalStateException(e);
        }
      }
    }
  }
}
