package MapReduce;

import java.io.IOException;
import java.text.Format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Phone extends Configured implements Tool {

	
	static Configuration configuration=new Configuration();
	
	enum Counter{
		
		LINESKIP,//出錯行
	}
	
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			try {
				String string = value.toString();
				String[] split = string.split(" ");
				String from=split[0];
				String to=split[1];
			    context.write(new Text(to), new Text(from));
			} catch (java.lang.ArrayIndexOutOfBoundsException e) {
				// TODO Auto-generated catch block
				context.getCounter(Counter.LINESKIP).increment(1);
				return;
			}
			
			
		}
		
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		
		@Override
		public void reduce(Text arg0, Iterable<Text> arg1,
				Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			
			String valueString;
			String out="";
			for(Text text:arg1){
				valueString=text.toString();
				out+=valueString+"|";
			}
			
			arg2.write(arg0, new Text(out));
			
		}
		
	}
	
	
	

	@Override
	public int run(String[] arg0) throws Exception {
		
		Job job=new Job(configuration, "phone");
		job.setJarByClass(Phone.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(arg0[0])); //文件輸入路徑
		FileOutputFormat.setOutputPath(job, new Path(arg0[1])); //文件輸出

		return job.waitForCompletion(true)?0:1;
		
		
	}
	
	
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res=ToolRunner.run(configuration, new Phone(), args);
		System.exit(res);
		
		
	}










}
