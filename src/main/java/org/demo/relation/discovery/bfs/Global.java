package org.demo.relation.discovery.bfs;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Global {
    private static volatile ApplicationContext appCtx;

    private static void init() {
    }

    public static ApplicationContext getAppCtx() {
        if (appCtx != null) {
            return appCtx;
        } else {
            synchronized (Global.class) {
                if (appCtx != null) {
                    return appCtx;
                } else {
                    init();
                    appCtx = new AnnotationConfigApplicationContext("com.vector.dacp.dp.bfs");
                    return appCtx;
                }
            }
        }
    }

}