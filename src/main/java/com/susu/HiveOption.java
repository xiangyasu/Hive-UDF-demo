package com.susu;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class HiveOption {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://alinode01:10000/testdb";

    private static Connection con = null;
    private static Statement state = null;
    private static ResultSet res = null;

    //加载驱动,创建连接
    @Before
    public void init() throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        con = DriverManager.getConnection(url, "root", "");
        state = con.createStatement();
    }

    //创建数据库
    @Test
    public void CreateDb() throws SQLException {

        state.execute("create database testdb2");

    }

    // 查询所有数据库
    @Test
    public void showtDb() throws SQLException {
        res = state.executeQuery("show databases");
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }

    // 删除数据库
    @Test
    public void dropDb() throws SQLException {
        state.execute("drop database if exists testdb2");
    }


    /*
     *
     *
     * 内部表基本操作
     *
     *
     * */

    // 创建表
    @Test
    public void createTab() throws SQLException {

        state.execute("create table if not exists student ( " +
                "name string , " +
                "age int , " +
                "agent string  ," +
                "adress struct<street:STRING,city:STRING>) " +
                "row format delimited " +
                "fields terminated by ',' " +//字段与字段之间的分隔符
                "collection items terminated by ':'" +//一个字段各个item的分隔符
                "lines terminated by '\n' ");//行分隔符
    }

    // 查询所有表
    @Test
    public void showTab() throws SQLException {
        res = state.executeQuery("show tables");
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }

    // 查看表结构
    @Test
    public void descTab() throws SQLException {
        res = state.executeQuery("desc student");
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
    }

    // 加载数据
    @Test
    public void loadData() throws SQLException {
        String infile = " '/root/studentData' ";
        state.execute("load data local inpath " + infile + "overwrite into table student");
    }

    // 查询数据
    @Test
    public void selectTab() throws SQLException {
        res = state.executeQuery("select * from student1");
        while (res.next()) {
            System.out.println(
                    res.getString(1) + "-" +
                            res.getString(2) + "-" +
                            res.getString(3) + "-" +
                            res.getString(4));
        }
    }

    // 统计查询（会运行mapreduce作业，资源开销较大）
    @Test
    public void countData() throws SQLException {
        res = state.executeQuery("select count(1) from student");
        while (res.next()) {
            System.out.println(res.getInt(1));
        }
    }

    // 删除表
    @Test
    public void dropTab() throws SQLException {
        state.execute("drop table emp");
    }


    /*
     * 外部表基本操作
     *
     *外部表删除后，hdfs文件系统上的数据还在，
     *重新创建同路径外部表后，其数据仍然存在
     *
     * */

    //创建外部表
    @Test
    public void createExTab() throws SQLException {

        state.execute("create external table if not exists student1 ( " +
                "name string , " +
                "age int , " +
                "agent string  ," +
                "adress struct<street:STRING,city:STRING>) " +
                "row format delimited " +
                "fields terminated by ',' " +
                "collection items terminated by ':'" +
                "lines terminated by '\n' " +
                "stored as textfile " +
                "location '/testData/hive/student1' ");//不指定路径时默认使用hive.metastore.warehouse.dir指定的路径
    }

    //从一张已经存在的表上复制其表结构，并不会复制其数据
    //
    //创建表，携带数据
    //create table student1 as select * from student
    //创建表，携带表结构
    //create table student1 like student
    //
    @Test
    public void copyExTab() throws SQLException {
        state.execute("create external table if not exists student2 " +
                "like xavierdb.student " +
                "location '/testData/hive/student1'");
    }


    /*
     * 分区表
     *
     * 必须在表定义时创建partition
     *
     *
     * */

    //静态分区


    //创建分区格式表
    @Test
    public void creatPartab() throws SQLException {
        state.execute("create table if not exists emp (" +
                "name string ," +
                "salary int ," +
                "subordinate array<string> ," +
                "deductions map<string,float> ," +
                "address struct<street:string,city:string>) " +
                "partitioned by (city string,street string) " +
                "row format delimited " +
                "fields terminated by '\t' " +
                "collection items terminated by ',' " +
                "map keys terminated by ':' " +
                "lines terminated by '\n' " +
                "stored as textfile");
    }

    //添加分区表
    @Test
    public void addPartition() throws SQLException {
        state.execute("alter table emp add partition(city='shanghai',street='jinkelu') ");
    }

    //查看分区表信息
    @Test
    public void showPartition() throws SQLException {
//        res=state.executeQuery("select * from emp");
        res = state.executeQuery("show partitions emp");
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }

    //插入数据
    @Test
    public void loadParData() throws SQLException {
        String filepath = " '/root/emp' ";
        state.execute("load data local inpath " + filepath + " overwrite into table emp partition (city='shanghai',street='jinkelu')");
    }

    //删除分区表
    @Test
    public void dropPartition() throws SQLException {
        state.execute("alter   table employees drop partition (city='shanghai',street='jinkelu') ");
        /*
        *
        * 1，把一个分区打包成一个har包
             alter table emp archive partition (city='shanghai',street='jinkelu')
          2, 把一个分区har包还原成原来的分区
    `        alter table emp unarchive partition (city='shanghai',street='jinkelu')
          3, 保护分区防止被删除
             alter table emp partition (city='shanghai',street='jinkelu') enable no_drop
          4,保护分区防止被查询
             alter table emp partition (city='shanghai',street='jinkelu') enable offline
          5，允许分区删除和查询
             alter table emp partition (city='shanghai',street='jinkelu') disable no_drop
             alter table emp partition (city='shanghai',street='jinkelu') disable offline
        * */
    }
    //外部表同样可以使用分区


    //动态分区
    //
    //当需要一次插入多个分区的数据时，可以使用动态分区，根据查询得到的数据动态分配到分区里。
    // 动态分区与静态分区的区别就是不指定分区目录，由hive根据实际的数据选择插入到哪一个分区。
    //
    //set hive.exec.dynamic.partition=true; 启动动态分区功能
    //set hive.exec.dynamic.partition.mode=nonstrict   分区模式，默认nostrict
    //set hive.exec.max.dynamic.partitions=1000       最大动态分区数,默认1000

    //创建分区格式表
    @Test
    public void creatPartab1() throws SQLException {
        state.execute("create table if not exists emp1 (" +
                        "name string ," +
                        "salary int ," +
                        "subordinate array<string> ," +
                        "deductions map<string,float> ," +
                        "address struct<street:string,city:string>) " +
                        "partitioned by (city string,street string) " +
                        "row format delimited " +
                        "fields terminated by '\t' " +
                        "collection items terminated by ',' " +
                        "map keys terminated by ':' " +
                        "lines terminated by '\n' " +
                        "stored as textfile");
    }

    //靠查询到的数据来分区
    @Test
    public void loadPartitionData() throws SQLException {
        state.execute("insert overwrite  table emp1 partition (city='shanghai',street) " +
                "select name,salary,subordinate,deductions,address,address.street from emp");
    }


    // 释放资源
    @After
    public void destory() throws SQLException {
        if (res != null) state.close();
        if (state != null) state.close();
        if (con != null) con.close();
    }
}