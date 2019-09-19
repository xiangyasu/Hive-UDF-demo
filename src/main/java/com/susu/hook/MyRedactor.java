package com.susu.hook;

import org.apache.hadoop.hive.ql.hooks.Redactor;

public class MyRedactor extends Redactor {

    @Override
    public String redactQuery(String query) {
        System.out.println("调用MyRedactor.redactQuery");
        return super.redactQuery(query);
    }
}
