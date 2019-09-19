package com.susu.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class NameUDF extends UDF {
    public int a = 0;
    public String evaluate(String name) {
        a++;
        return name + "**" + a ;
    }
}
