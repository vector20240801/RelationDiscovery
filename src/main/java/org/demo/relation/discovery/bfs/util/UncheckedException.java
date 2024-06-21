package org.demo.relation.discovery.bfs.util;

/**
 * 带状态的、非检查异常
 *
 * @author vector
 * @date 2017/8/3 10:43
 */
public class UncheckedException extends RuntimeException {

    private static final long serialVersionUID = 3055624593557992428L;

    private Object state;

    public UncheckedException() {
    }

    public UncheckedException(String message) {
        super(message);
    }

    public UncheckedException(String message, Throwable cause) {
        super(message, Throwables.unwrap(cause));
    }

    public UncheckedException(Throwable cause) {
        super(Throwables.unwrap(cause));
    }

    /**
     * 设置状态
     *
     * @param state
     * @return
     */
    public UncheckedException state(Object state) {
        this.state = state;
        return this;
    }

    /**
     * 获取状态
     *
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T> T state() {
        return (T) state;
    }
}
