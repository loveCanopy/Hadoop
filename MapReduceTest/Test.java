package MapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import wordCount.WordCount;
import wordCount.WordCount.IntSumReducer;
import wordCount.WordCount.TokenizerMapper;

public class Test extends Configured implements Tool{

	
	
	static Configuration conf = new Configuration();
	
	enum Counter{
	
		LINESKIP,//���e��
	}
	
	
	//Map
	
	//ݔ��key value  ݔ��key value
	
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text>{
		
		@Override
		public void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			try {
				 String string = value.toString();
				 String[] split = string.split(" ");
				 String month=split[0];
				 String time=split[1];
				 String mac=split[6];
				 Text out=new Text(month+time+mac);
				 context.write(NullWritable.get(), out);
			} catch (java.lang.ArrayIndexOutOfBoundsException e) {
				// TODO: handle exception
				context.getCounter(Counter.LINESKIP).increment(1);
				return;
			}
		  
		}
		
		
	}
	
	
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		   
		    Job job = new Job(conf, "test"); //ָ���΄���

		    job.setJarByClass(Test.class); //ָ��Class

		    job.setMapperClass(Map.class);//�{�������map�
//
//		    job.setCombinerClass(IntSumReducer.class);
//
//		    job.setReducerClass(IntSumReducer.class);//�{�������reduce�

		    job.setOutputKeyClass(NullWritable.class); //ָ��ݔ��key�ĸ�ʽ

		    job.setOutputValueClass(Text.class); //ָ��ݔ��value�ĸ�ʽ

		    FileInputFormat.addInputPath(job, new Path(arg0[0])); //�ļ�ݔ��·��

		    FileOutputFormat.setOutputPath(job, new Path(arg0[1])); //�ļ�ݔ��

		return job.waitForCompletion(true) ? 0 : 1;
	}

	
	public static void main(String[] args) throws Exception {
		
		int res=ToolRunner.run(conf, new Test(), args);
		System.exit(res);
		
	}
	
	
}
