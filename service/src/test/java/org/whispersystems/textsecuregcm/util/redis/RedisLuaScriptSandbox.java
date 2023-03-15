/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.io.Resources;
import io.lettuce.core.ScriptOutputType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import party.iroiro.luajava.Lua;
import party.iroiro.luajava.lua51.Lua51;
import party.iroiro.luajava.value.ImmutableLuaValue;

public class RedisLuaScriptSandbox {

  private static final String PREFIX = """
      function redis_call(...)
        -- variable name needs to match the one used in the `L.setGlobal()` call
        -- method name needs to match method name of the Java class 
        local result = proxy:redisCall(arg)
        if type(result) == "userdata" then
          return java.luaify(result)
        else
          return result
        end
      end
      
      function json_encode(obj)
        return mapper:encode(obj)
      end
      
      function json_decode(json)
        return java.luaify(mapper:decode(json))
      end
      
      local redis = { call = redis_call }
      local cjson = { encode = json_encode, decode = json_decode }
      
      """;

  private final String luaScript;

  private final ScriptOutputType scriptOutputType;


  public static RedisLuaScriptSandbox fromResource(
      final String resource,
      final ScriptOutputType scriptOutputType) {
    try {
      final String src = Resources.toString(Resources.getResource(resource), StandardCharsets.UTF_8);
      return new RedisLuaScriptSandbox(src, scriptOutputType);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public RedisLuaScriptSandbox(final String luaScript, final ScriptOutputType scriptOutputType) {
    this.luaScript = luaScript;
    this.scriptOutputType = scriptOutputType;
  }

  public Object execute(
      final List<String> keys,
      final List<String> args,
      final RedisCommandsHandler redisCallsHandler) {

    try (final Lua lua = new Lua51()) {
      lua.openLibraries();
      final RedisLuaProxy proxy = new RedisLuaProxy(redisCallsHandler);
      lua.push(MapperLuaProxy.INSTANCE, Lua.Conversion.FULL);
      lua.setGlobal("mapper");
      lua.push(proxy, Lua.Conversion.FULL);
      lua.setGlobal("proxy");
      lua.push(keys, Lua.Conversion.FULL);
      lua.setGlobal("KEYS");
      lua.push(args, Lua.Conversion.FULL);
      lua.setGlobal("ARGV");
      final Lua.LuaError executionResult = lua.run(PREFIX + luaScript);
      assertEquals("OK", executionResult.name(), "Runtime error during Lua script execution");
      return adaptOutputResult(lua.get());
    }
  }

  protected Object adaptOutputResult(final Object luaObject) {
    if (luaObject instanceof ImmutableLuaValue<?> luaValue) {
      final Object javaValue = luaValue.toJavaObject();
      // validate expected script output type
      switch (scriptOutputType) {
        case INTEGER -> assertTrue(javaValue instanceof Double); // lua number is always Double
        case STATUS -> assertTrue(javaValue instanceof String);
        case BOOLEAN -> assertTrue(javaValue instanceof Boolean);
      };
      if (javaValue instanceof Double d) {
        return d.longValue();
      }
      if (javaValue instanceof String s) {
        return s;
      }
      if (javaValue instanceof Boolean b) {
        return b;
      }
      if (javaValue == null) {
        return null;
      }
      throw new IllegalStateException("unexpected script result java type: " + javaValue.getClass().getName());
    }
    throw new IllegalStateException("unexpected script result lua type: " + luaObject.getClass().getName());
  }

  public static <T> List<T> tail(final List<T> list, final int fromIdx) {
    return fromIdx < list.size() ? list.subList(fromIdx, list.size()) : Collections.emptyList();
  }

  public static final class MapperLuaProxy {

    public static final MapperLuaProxy INSTANCE = new MapperLuaProxy();

    public String encode(final Map<Object, Object> obj) {
      try {
        return SystemMapper.jsonMapper().writeValueAsString(obj);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    public Map<Object, Object> decode(final Object json) {
      try {
        //noinspection unchecked
        return SystemMapper.jsonMapper().readValue(json.toString(), Map.class);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Instances of this class are passed to the Lua scripting engine
   * and serve as a stubs for the calls to `redis.call()`.
   *
   * @see #PREFIX
   */
  public static final class RedisLuaProxy {

    private final RedisCommandsHandler handler;

    public RedisLuaProxy(final RedisCommandsHandler handler) {
      this.handler = handler;
    }

    /**
     * Method name needs to match the one from the {@link #PREFIX} code.
     * The method is getting called from the Lua scripting engine.
     */
    @SuppressWarnings("unused")
    public Object redisCall(final List<Object> args) {
      assertFalse(args.isEmpty(), "`redis.call()` in Lua script invoked without arguments");
      assertTrue(args.get(0) instanceof String, "first argument to `redis.call()` must be of type `String`");
      return handler.redisCommand((String) args.get(0), tail(args, 1));
    }
  }
}
