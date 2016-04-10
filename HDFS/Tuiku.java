package wordCount;



import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class Tuiku {
    private  Configuration conf = new Configuration();//这里创建conf对象有一个默认参数，boolean loadDefaults，默认为true
    private String rootPath=new String("hdfs://192.168.2.6:9000/");
    private FileSystem coreSys=null;
    /**
     * 每次执行之前初始化操作，初始化FileSystem核心对象
     */
    @Before
    public void iniFileSystemObject(){
        try {
            coreSys=FileSystem.get(URI.create(rootPath), conf);
        } catch (IOException e) {
            System.out.println("初始化HDFS核心文件对象失败："+e.getLocalizedMessage());
        }
    }
    /**
     * 在HDFS上创建文件目录
     */
    @Test  @Ignore
    public void createDirOnHDFS(){
           Path demoDir=new Path(rootPath+"/user/root/demoDir");
           boolean isSuccess=true;
           try {
               isSuccess=coreSys.mkdirs(demoDir);
           } catch (IOException e) {
               isSuccess=false;
           }
           System.out.println(isSuccess?"目录创建成功！":"目录创建失败!");
           
    }
    /**
     * 在HDFS上创建文件
     * @throws Exception 
     */
    @Test @Ignore
    public void createFile() throws Exception{
                Path hdfsPath = new Path(rootPath + "user/root/createDemoFile");
                System.out.println(coreSys.getHomeDirectory());
                String content = "Hello hadoop,this is first time that I create file on hdfs";
                FSDataOutputStream fsout = coreSys.create(hdfsPath);
                BufferedOutputStream bout = new BufferedOutputStream(fsout);
                bout.write(content.getBytes(), 0, content.getBytes().length);
                bout.close();
                fsout.close();
                System.out.println("文件创建完毕！");
    }
    /**
     * 从本地上传任意文件到服务器HDFS环境
     * @throws Exception
     */
    @Test  @Ignore
    public void uploadFile() throws Exception{
         Configuration conf = new Configuration();
         Path remotePath=new Path(rootPath+"/user/root");
         coreSys.copyFromLocalFile(new Path("D:\\a.txt"), remotePath);
         System.out.println("Upload to:"+conf.get("fs.default.name"));
         FileStatus [] files=coreSys.listStatus(remotePath);
         for(FileStatus file:files){
             System.out.println(file.getPath().toString());
         }
    }
    /**
     * 重命名文件名
     */
    @Test @Ignore
    public void renameFile(){
        Path oldFileName=new Path(rootPath+"user/root/createDemoFile");
        Path newFileName=new Path(rootPath+"user/root/renameDemoFile");
        boolean isSuccess=true;
        try {
            isSuccess=coreSys.rename(oldFileName, newFileName);
        } catch (IOException e) {
             isSuccess=false;
        }
        System.out.println(isSuccess?"重命名成功！":"重命名失败！");
    }
    /**
     * 删除文件
     */
    @Test  @Ignore
    public void deleteFile(){
        Path deleteFile=new Path(rootPath+"user/root/demoDir");
        boolean isSuccess=true;
        try {
            isSuccess=coreSys.delete(deleteFile, false);
        } catch (IOException e) {
            isSuccess=false;
        }
        System.out.println(isSuccess?"删除成功!":"删除失败！");
    }
    /**
     * 查找某个文件是否存在
     */
    @Test @Ignore
    public void findFileIsExit(){
          Path checkFile=new Path(rootPath+"user/root/job.jar");
          boolean isExit=true;
          try {
              isExit=coreSys.exists(checkFile);
            } catch (IOException e) {
                isExit=false;
           }
        System.out.println(isExit?"文件存在!":"文件不存在！");
    }
    /**
     * 查看某个文件的最后修改时间
     * @throws IOException 
     */
    @Test @Ignore
    public void watchFileLastModifyTime() throws IOException{
         Path targetFile=new Path(rootPath+"user/hdfsupload/renameDemoFile");
         FileStatus fileStatus=coreSys.getFileStatus(targetFile);
         Long lastTime=fileStatus.getModificationTime();
         Date date=new Date(lastTime);
         SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
         System.err.println("文件的最后修改时间为:"+format.format(date));
    }
    /**
     * 获取某个路径下面的所有文件
     * @throws IOException 
     */
    @Test @Ignore
    public void getUnderDirAllFile() throws IOException{
        Path targetDir=new Path(rootPath+"user/hdfsupload/");
        FileStatus []fileStatus=coreSys.listStatus(targetDir);
        for(FileStatus file:fileStatus){
            System.out.println(file.getPath()+"--"+file.getGroup()+"--"+file.getBlockSize()+"--"+file.getLen()+"--"+file.getModificationTime()+"--"+file.getOwner());
        }
    }
    /**
     * 查看某个文件在HDFS集群的位置
     * @throws IOException 
     */
    @Test  @Ignore
    public void findLocationOnHadoop() throws IOException{
        Path targetFile=new Path(rootPath+"user/root/a.txt");
        FileStatus fileStaus=coreSys.getFileStatus(targetFile);
        BlockLocation []bloLocations=coreSys.getFileBlockLocations(fileStaus, 0, fileStaus.getLen());
        for(int i=0;i<bloLocations.length;i++){
            System.out.println("block_"+i+"_location:"+bloLocations[i].getHosts()[0]);
        }
        
    }
    /**
     * 获取集群上结点的信息
     * @throws IOException 
     * 
     * 
     * DataNode_0_Name:master--->Name: 192.168.2.6:50010
Decommission Status : Normal
Configured Capacity: 18427674624 (17.16 GB)
DFS Used: 155648 (152 KB)
Non DFS Used: 1505615872 (1.4 GB)
DFS Remaining: 16921903104(15.76 GB)
DFS Used%: 0%
DFS Remaining%: 91.83%
Last contact: Sun Apr 10 08:44:46 CST 2016
-->8.4464264E-4-->0

     */
    @Test @Ignore
    public void getNodeMsgHdfs() throws IOException{
         DistributedFileSystem distributedFileSystem=(DistributedFileSystem) coreSys;
         DatanodeInfo []dataInfos=distributedFileSystem.getDataNodeStats();
         for(int j=0;j<dataInfos.length;j++){
             System.out.println("DataNode_"+j+"_Name:"+dataInfos[j].getHostName()+"--->"+dataInfos[j].getDatanodeReport()+"-->"+
            dataInfos[j].getDfsUsedPercent()+"-->"+dataInfos[j].getLevel());
         }
    }
    
   
    
}