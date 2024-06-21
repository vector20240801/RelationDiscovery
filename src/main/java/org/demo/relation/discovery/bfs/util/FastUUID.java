package org.demo.relation.discovery.bfs.util;

import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 比java.util.UUID更快的实现，参见：MongoDB的ObjectId。
 *
 * @author vector
 * @date 2017/8/3 11:14
 */
public final class FastUUID {

    private static final Logger LOG = LoggerFactory.getLogger(FastUUID.class);

    // 每秒内的自增器
    private static final AtomicInteger COUNTER = new AtomicInteger(new SecureRandom().nextInt());
    // 进程ID
    private static final short PROCESS_ID = getPid();
    // 3个字节（24位）的最大容量
    private static final int BIT24_MASK = (1 << 24) - 1;
    // MAC地址
    private static final int MAC_ID = getMacId();

    private FastUUID() {
    }

    /**
     * 获取UUID
     *
     * @return
     */
    public static final String next() {
        int now = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        int counter = COUNTER.getAndIncrement() & BIT24_MASK;

        ByteBuffer bb = ByteBuffer.allocate(12);
        bb.put(int3(now));
        bb.put(int2(now));
        bb.put(int1(now));
        bb.put(int0(now));
        bb.put(int2(MAC_ID));
        bb.put(int1(MAC_ID));
        bb.put(int0(MAC_ID));
        bb.put(short1(PROCESS_ID));
        bb.put(short0(PROCESS_ID));
        bb.put(int2(counter));
        bb.put(int1(counter));
        bb.put(int0(counter));
        return BaseEncoding.base16().lowerCase().encode(bb.array());
    }

    private final static byte int3(int x) {
        return (byte) (x >> 24);
    }

    private final static byte int2(int x) {
        return (byte) (x >> 16);
    }

    private final static byte int1(int x) {
        return (byte) (x >> 8);
    }

    private final static byte int0(int x) {
        return (byte) x;
    }

    private final static byte short1(short x) {
        return (byte) (x >> 8);
    }

    private final static byte short0(short x) {
        return (byte) x;
    }

    // 获取MAC地址,如果获取不到,则使用随机数
    private static final int getMacId() {
        int macId = 0;
        StringBuilder builder = new StringBuilder();
        byte macs[][] = Platforms.MACS;
        if (macs.length == 0) { // 获取不到mac地址
            macId = new SecureRandom().nextInt();
            LOG.warn("Failed to get machine identifier from network interface, use random number instead");
        } else {
            for (int i = 0; i < macs.length; i++) {
                byte mac[] = macs[i];
                ByteBuffer buf = ByteBuffer.wrap(mac);
                try {
                    builder.append(buf.getChar());
                    builder.append(buf.getChar());
                    builder.append(buf.getChar());
                } catch (BufferUnderflowException e) {
                    // ignore
                }
            }
            macId = builder.toString().hashCode();
        }

        macId &= BIT24_MASK;
        return macId;
    }

    // 获取进程ID,如果获取不到,则使用随机数
    private static final short getPid() {
        int pid = Platforms.PID;
        if (pid < 0) {
            pid = new SecureRandom().nextInt();
            LOG.warn("Failed to get process identifier from JMX, use random number instead");
        }

        return (short) pid;
    }
}
