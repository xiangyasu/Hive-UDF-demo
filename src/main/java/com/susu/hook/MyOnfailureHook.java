package com.susu.hook;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;

public class MyOnfailureHook implements ExecuteWithHookContext {
    @Override
    public void run(HookContext hookContext) throws Exception {
        System.out.println("调用MyOnfailureHook.run");
    }
}
