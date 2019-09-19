package com.susu.hook;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.Serializable;
import java.util.List;

public class MyAnalyzeHook implements HiveSemanticAnalyzerHook {
    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext hiveSemanticAnalyzerHookContext, ASTNode astNode) throws SemanticException {
        System.out.println("调用MyAnalyzeHook.preAnalyze");
        return astNode;
    }

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext hiveSemanticAnalyzerHookContext, List<Task<? extends Serializable>> list) throws SemanticException {
        System.out.println("调用MyAnalyzeHook.postAnalyze");
    }
}
