package org.demo.relation.discovery.bfs.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * 获取平台相关信息
 *
 * @Deprecated 用 RuntimeContext
 * @date 2017/8/3 11:21
 */

public final class Platforms {

    private static final Logger LOG = LoggerFactory.getLogger(Platforms.class);

    /**
     * 文件路径分隔符
     */
    public static final String FILE_PATH_SEPARATOR = File.separator;
    /**
     * 文件路径分隔符（字符）
     */
    public static final char FILE_PATH_SEPARATOR_CHAR = File.separatorChar;
    /**
     * 系统换行符
     */
    public static final String LINE_SEPARATOR = System.lineSeparator();
    /**
     * ClassPath分隔符（*nix下是“:”，windows下是“;”）
     */
    public static final String CLASSPATH_SEPARATOR = File.pathSeparator;
    /**
     * ClassPath分隔符（字符）（*nix下是“:”，windows下是“;”）
     */
    public static final char CLASSPATH_SEPARATOR_CHAR = File.pathSeparatorChar;
    /**
     * 临时目录
     */
    public static final String TMP_DIR = SystemUtils.JAVA_IO_TMPDIR;
    /**
     * 项目运行目录
     */
    public static final String WORKING_DIR = SystemUtils.USER_DIR;
    /**
     * 用户主目录
     */
    public static final String USER_DIR = SystemUtils.USER_HOME;
    /**
     * Java目录
     */
    public static final String JAVA_HOME = SystemUtils.JAVA_HOME;
    /**
     * Java版本
     */
    public static final String JAVA_VERSION = SystemUtils.JAVA_SPECIFICATION_VERSION;
    /**
     * 是否为Java1.6
     */
    public static final boolean IS_JAVA6 = SystemUtils.IS_JAVA_1_6;
    /**
     * 是否为Java1.7
     */
    public static final boolean IS_JAVA7 = SystemUtils.IS_JAVA_1_7;
    /**
     * 是否为Java1.8
     */
    public static final boolean IS_JAVA8 = SystemUtils.IS_JAVA_1_8;
    /**
     * 操作系统名称
     */
    public static final String OS_NAME = SystemUtils.OS_NAME;
    /**
     * 操作系统版本
     */
    public static final String OS_VERSION = SystemUtils.OS_VERSION;
    /**
     * 操作系统架构（x32、x64）
     */
    public static final String OS_ARCH = SystemUtils.OS_ARCH;
    /**
     * 当前系统是否为Linux
     */
    public static final boolean IS_LINUX = SystemUtils.IS_OS_LINUX;
    /**
     * 当前系统是否为Unix
     */
    public static final boolean IS_UNIX = SystemUtils.IS_OS_UNIX;
    /**
     * 当前操作系统是否为MacOS
     */
    public static final boolean IS_MAC = SystemUtils.IS_OS_MAC;
    /**
     * 当前系统是否为Windows
     */
    public static final boolean IS_WINDOWS = SystemUtils.IS_OS_WINDOWS;
    /**
     * 系统进程号，如果获取失败则为-1
     */
    public static final int PID;
    /**
     * 所有网卡的mac地址
     */
    public static final byte[][] MACS;
    /**
     * 本机IP
     */
    public static String LOCAL_IP;
    /**
     * 处于 测试、预发、还是线上环境
     */

    public static boolean RUNTIMECONTXT_VALID;

    static {
        PID = getPid();
        MACS = getMacs();
        LOCAL_IP = getLocalIp();
        RUNTIMECONTXT_VALID = checkRuntimeContext();
    }

    private Platforms() {

    }


    /**
     * 是否在开发机上启动
     */
    public static boolean isInDevPc() {
        return IS_WINDOWS || IS_MAC;
    }

    private static final int getPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        String parts[] = name.split("@");
        if (parts.length != 2) return -1;
        try {
            return Integer.parseInt(parts[0]);
        } catch (Exception e) {
            LOG.warn("get pid error", e);
            return -1;
        }
    }

    private static final byte[][] getMacs() {
        List<byte[]> macs = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> networks = NetworkInterface.getNetworkInterfaces();
            while (networks.hasMoreElements()) {
                NetworkInterface network = networks.nextElement();
                byte mac[] = network.getHardwareAddress();
                if (mac != null) macs.add(mac);
            }
        } catch (Exception e) {
            LOG.warn("get macs error", e);
            return new byte[0][0];
        }
        return macs.toArray(new byte[0][0]);
    }

    private static final String getLocalIp() {
        try {
            Enumeration<NetworkInterface> networks = NetworkInterface.getNetworkInterfaces();
            while (networks.hasMoreElements()) {
                NetworkInterface network = networks.nextElement();
                if (network.isLoopback() || network.isVirtual() || !network.isUp()) {
                    continue;
                }

                Enumeration<InetAddress> addresses = network.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address.isLoopbackAddress() || address instanceof Inet6Address) {
                        continue;
                    }

                    return address.getHostAddress();
                }
            }
        } catch (Exception e) {
            LOG.warn("get local ip error", e);
        }


        return "UNKNOWN-LOCAL-IP";
    }

    public static boolean checkRuntimeContext() {
        return StringUtils.isEmpty(System.getProperty("vector.env"));
    }


}
