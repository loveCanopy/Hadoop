package MapreduceHbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MainHbase {

	public static void main(String[] args) {
		
		try {
			Configuration configuration=HBaseConfiguration.create();
			configuration.set("hbase.zookeeper.quorum", "192.168.2.6");
			configuration.set("hbase.zookeeper.property.clientPort", "2181"); 
			configuration.set("hbase.master", "192.168.2.6:54311"); 
			HBaseAdmin baseAdmin=new HBaseAdmin(configuration);
			String tableName="wordcount";
			if(baseAdmin.tableExists(tableName)){
				baseAdmin.disableTable(tableName);
				baseAdmin.deleteTable(tableName);
			}
      HTableDescriptor hTableDescriptor=new HTableDescriptor(tableName);
      HColumnDescriptor hColumnDescriptor=new HColumnDescriptor("content");
      hTableDescriptor.addFamily(hColumnDescriptor);
      baseAdmin.createTable(hTableDescriptor);
			String[] otherArgs=new GenericOptionsParser(configuration, args).getRemainingArgs();
			if(otherArgs.length!=1){
				System.err.println("Usge:worldcount"+otherArgs.length);
			}
			Job job=new Job(configuration,"wordcount-hbase");
			job.setMapperClass(MapHbase.class);
			job.setJarByClass(MainHbase.class);
     		
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			TableMapReduceUtil.initTableReducerJob(tableName, ReduceHbase.class, job);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			System.exit(job.waitForCompletion(true)? 0:1);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
}
