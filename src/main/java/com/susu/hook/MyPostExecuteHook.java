package com.susu.hook;

import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.PostExecute;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Set;

public class MyPostExecuteHook implements PostExecute {
    @Override
    public void run(SessionState sessionState, Set<ReadEntity> set, Set<WriteEntity> set1, LineageInfo lineageInfo, UserGroupInformation userGroupInformation) throws Exception {
        System.out.println("调用MyPostExecuteHook.run");
    }
}
