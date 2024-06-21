package org.demo.relation.discovery.bfs.util;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;

/**
 * 异常相关工具类
 *
 * @author vector
 * @date 2017/8/3 10:46
 */
public final class Throwables {

    private Throwables() {
    }

    /**
     * 将检查异常包装成非检查异常（UncheckedException）
     *
     * @param t
     */
    public static final void unchecked(Throwable t) {
        if (t instanceof UncheckedException) {
            throw (UncheckedException) t;
        }

        if (t instanceof Error) {
            throw (Error) t;
        }

        throw new UncheckedException(t);
    }

    /**
     * 如果异常为ExecutionException、InvocationTargetException、UncheckedException，则从cause获取真正的异常。
     *
     * @param t
     * @return
     */
    public static final Throwable unwrap(Throwable t) {
        if (t instanceof ExecutionException || t instanceof InvocationTargetException || t instanceof
                UncheckedException) {
            return t.getCause();
        }

        return t;
    }

    /**
     * 获取异常堆栈信息
     *
     * @param t
     * @return
     */
    public static final String getStackTrace(Throwable t) {
        StringWriter buffer = new StringWriter(128);
        t.printStackTrace(new PrintWriter(buffer));
        return buffer.toString();
    }

    /**
     * 判断异常是否由指定的底层异常引起的
     *
     * @param t
     * @param causeClasses
     * @return
     */
    @SafeVarargs
    public static final boolean isCausedBy(Throwable t, Class<? extends Throwable>... causeClasses) {
        Throwable cause = t;
        while (cause != null) {
            for (int i = 0; i < causeClasses.length; i++) {
                if (causeClasses[i].isInstance(cause)) {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
    }

    /**
     * 获取异常的简短描述，描述格式为："ShortClassName: Message"
     *
     * @param t
     * @return
     */
    public static final String getShortMessage(Throwable t) {
        if (t == null) {
            return "";
        }

        String className = ClassUtils.getShortClassName(t.getClass());
        String message = StringUtils.defaultString(t.getMessage());
        return className + ": " + message;
    }
}
