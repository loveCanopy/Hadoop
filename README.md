# Hadoop
Hadoop.hdfs.hbase
## 常见问题
### Hadoop伪分布式安装
http://www.cnblogs.com/fabulousyoung/p/4074197.html
http://www.cnblogs.com/lanxuezaipiao/p/3525554.html
### 常用API
http://hadoop.apache.org/docs/r1.2.1/api/index.html
### HbaseAPI
https://hbase.apache.org/apidocs/index.html
### Hadoop管理页面
http://192.168.2.6:60010/master-status
### eclipse远程开发hbase是报 unknown host错误
1.首先配置好Zookeeper的configuration信息
configuration = HBaseConfiguration.create();  
configuration.set("hbase.zookeeper.quorum", "192.168.2.6");
configuration.set("hbase.zookeeper.property.clientPort", "2181"); 
configuration.set("hbase.master", "192.168.2.6:54311"); 
2.若Hbase在linux，eclipse在Windows
类似异常信息如下：java.net.UnknownHostException: unknown host: master
解决办法如下：在C:\WINDOWS\system32\drivers\etc\hosts文件中添加如下信息
192.168.2.34 master
### eclipse远程访问hdfs时，路径应像如下设置，否则无法连接
private String rootPath=new String("hdfs://192.168.2.6:9000/");
coreSys=FileSystem.get(URI.create(rootPath), conf);
Path demoDir=new Path(rootPath+"/user/root/demoDir");
### 执行mapreduce时，注意运行时指定参数
hdfs://192.168.2.6:9000/user/root/input/* hdfs://192.168.2.6:9000/user/root/output
