package com.susu.hook;



import org.apache.hadoop.hive.ql.hooks.PreExecute;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Set;

public class MyPreExecuteHook implements PreExecute {
    @Override
    public void run(SessionState sess, Set<ReadEntity> inputs,
                    Set<WriteEntity> outputs, UserGroupInformation ugi) throws Exception {
        System.out.println("调用MyPreExecuteHook.run");
    }
}
