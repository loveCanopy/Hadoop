package HDFS;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
import org.junit.Test;

public class App {

	

	static	Configuration conf=new Configuration();
	static	String rootPath=new String("hdfs://192.168.2.6:9000/");
	static	FileSystem fs=null;
	
	@Test @Ignore
	public void testName() throws Exception {
		fs=FileSystem.get(URI.create(rootPath),conf);
		
		Path srcpath=new Path(rootPath+"/user/root/test/*");
		//PathFilter是过滤布符合置顶表达式的路径，下列就是把以txt结尾的过滤掉
        FileStatus[] status = fs.globStatus(srcpath,new RegexExcludePathFilter(".*txt"));
        //FileStatus[] status = fs.globStatus(srcpath);
        Path[] listedPaths = FileUtil.stat2Paths(status);
        for (Path p : listedPaths) {
            System.out.println(p);
        }
    }
		
	@Test
	public void testNAME() throws Exception {
		// TODO Auto-generated method stub
        fs=FileSystem.get(URI.create(rootPath),conf);
		Path srcpath=new Path(rootPath+"/user/root/test/*");
		//PathFilter是过滤布符合置顶表达式的路径，下列就是把以txt结尾的过滤掉
        FileStatus[] status = fs.globStatus(srcpath,new FileFilter(".*txt"));
        //FileStatus[] status = fs.globStatus(srcpath);
        Path[] listedPaths = FileUtil.stat2Paths(status);
        for (Path p : listedPaths) {
            System.out.println(p);
        }
	}
	
	
	
	
	}
	

