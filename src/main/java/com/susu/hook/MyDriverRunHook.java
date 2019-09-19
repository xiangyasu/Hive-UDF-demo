package com.susu.hook;

import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;

public class MyDriverRunHook implements HiveDriverRunHook {
    @Override
    public void preDriverRun(HiveDriverRunHookContext hiveDriverRunHookContext) throws Exception {
        System.out.println("调用MyDriverRunHook.preDriverRun");
    }

    @Override
    public void postDriverRun(HiveDriverRunHookContext hiveDriverRunHookContext) throws Exception {
        System.out.println("调用MyDriverRunHook.postDriverRun");
    }
}
