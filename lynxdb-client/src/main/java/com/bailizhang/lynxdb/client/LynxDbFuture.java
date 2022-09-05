package com.bailizhang.lynxdb.client;

import com.bailizhang.lynxdb.client.exception.InvalidArgumentException;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.ClassUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.bailizhang.lynxdb.server.engine.result.Result.*;
import static com.bailizhang.lynxdb.server.engine.result.Result.Error.INVALID_ARGUMENT;

public class LynxDbFuture implements Future<byte[]> {
    private final static String SET_METHOD_PREFIX = "set";
    private final static String KEY_COLUMN = "key";

    private final Thread current;

    private volatile boolean completed = false;
    private volatile byte[] value;

    public LynxDbFuture() {
        current = Thread.currentThread();
    }

    public void value(byte[] val) {
        value = val;

        if(!completed) {
            completed = true;
            LockSupport.unpark(current);
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDone() {
        return completed;
    }

    @Override
    public byte[] get() {
        while (!completed) {
            LockSupport.park();
        }
        return value;
    }

    public <T> T get(Class<T> clazz) throws InvalidArgumentException, NoSuchMethodException,
            InvocationTargetException, InstantiationException, IllegalAccessException {

        byte[] value = get();
        ByteBuffer buffer = ByteBuffer.wrap(value);

        byte code = buffer.get();
        switch (code) {
            case SUCCESS_WITH_KV_PAIRS -> {
                T instance = clazz.getDeclaredConstructor().newInstance();
                HashMap<String, Method> methodMap = methodMap(clazz);

                while (BufferUtils.isNotOver(buffer)) {
                    String key = BufferUtils.getString(buffer);

                    String methodName = SET_METHOD_PREFIX + key.replace("_", "").toLowerCase();
                    Method method = methodMap.get(methodName);

                    if(method == null) {
                        continue;
                    }

                    Class<?> parameterType = method.getParameterTypes()[0];
                    Object val = getValue(buffer, parameterType);

                    method.invoke(instance, val);
                }

                return instance;
            }

            case INVALID_ARGUMENT -> {
                String message = BufferUtils.getRemainingString(buffer);
                throw new InvalidArgumentException(message);
            }

            default -> throw new UnsupportedOperationException();
        }
    }

    public List<String> getList() throws InvalidArgumentException {
        byte[] value = get();
        ByteBuffer buffer = ByteBuffer.wrap(value);

        byte code = buffer.get();
        switch (code) {
            case SUCCESS_WITH_LIST -> {
                List<String> list = new ArrayList<>();

                while (BufferUtils.isNotOver(buffer)) {
                    list.add(BufferUtils.getString(buffer));
                }

                return list;
            }

            case INVALID_ARGUMENT -> {
                String message = BufferUtils.getRemainingString(buffer);
                throw new InvalidArgumentException(message);
            }

            default -> throw new UnsupportedOperationException();
        }
    }

    public <T> List<T> getList(Class<T> clazz) throws InvalidArgumentException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        byte[] value = get();
        ByteBuffer buffer = ByteBuffer.wrap(value);

        byte code = buffer.get();
        switch (code) {
            case SUCCESS_WITH_TABLE -> {
                List<T> list = new ArrayList<>();

                int columnSize = buffer.getInt();
                List<String> columns = new ArrayList<>();

                columns.add(KEY_COLUMN);
                for(int i = 0; i < columnSize; i ++) {
                    columns.add(BufferUtils.getString(buffer));
                }

                HashMap<String, Method> methodMap = methodMap(clazz);

                while (BufferUtils.isNotOver(buffer)) {
                    T instance = clazz.getDeclaredConstructor().newInstance();

                    for(int i = 0; i < columnSize + 1; i ++) {
                        String column = KEY_COLUMN + columns.get(i).toLowerCase();

                        Method method = methodMap.get(column);
                        Class<?> parameterType = method.getParameterTypes()[0];
                        Object val = getValue(buffer, parameterType);

                        method.invoke(instance, val);
                    }

                    list.add(instance);
                }

                return list;
            }

            case INVALID_ARGUMENT -> {
                String message = BufferUtils.getRemainingString(buffer);
                throw new InvalidArgumentException(message);
            }

            default -> throw new UnsupportedOperationException();
        }
    }

    @Override
    public byte[] get(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    private Object getValue(ByteBuffer buffer, Class<?> parameterType) {
        Object val;

        if (ClassUtils.isString(parameterType)) {
            val = BufferUtils.getString(buffer);
        } else if (ClassUtils.isByte(parameterType)) {
            val = buffer.get();
        } else if (ClassUtils.isShort(parameterType)) {
            val = buffer.getShort();
        } else if (ClassUtils.isInt(parameterType)) {
            val = buffer.getInt();
        } else if (ClassUtils.isLong(parameterType)) {
            val = buffer.getLong();
        } else if (ClassUtils.isChar(parameterType)) {
            val = buffer.getChar();
        } else if (ClassUtils.isFloat(parameterType)) {
            val = buffer.getFloat();
        } else if (ClassUtils.isDouble(parameterType)) {
            val = buffer.getDouble();
        } else {
            throw new RuntimeException("Unsupported parameter type: " + parameterType.getName());
        }

        return val;
    }

    private HashMap<String, Method> methodMap(Class<?> clazz) {
        Method[] methods = clazz.getDeclaredMethods();
        HashMap<String, Method> methodMap = new HashMap<>();

        for(Method method : methods) {
            if(method.getParameterCount() != 1) {
                continue;
            }

            Class<?> parameterType = method.getParameterTypes()[0];
            if(!ClassUtils.isByte(parameterType)
                    && !ClassUtils.isInt(parameterType)
                    && !ClassUtils.isShort(parameterType)
                    && !ClassUtils.isLong(parameterType)
                    && !ClassUtils.isString(parameterType)
                    && !ClassUtils.isChar(parameterType)
                    && !ClassUtils.isFloat(parameterType)
                    && !ClassUtils.isDouble(parameterType)) {
                continue;
            }

            String methodName = method.getName().replace("_", "").toLowerCase();
            methodMap.put(methodName, method);
        }

        return methodMap;
    }
}
