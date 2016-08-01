import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MiniTemp {

	
	
	public static class MiniMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			String year=line.substring(15, 19);
			int airTem;
			if(line.charAt(87)=='+'){
				airTem=Integer.parseInt(line.substring(88, 92));
			}else{
				airTem=Integer.parseInt(line.substring(87, 92));
			}
			
			String quality=line.substring(92, 93);
			if(airTem!=9999&&quality.matches("[01459]")){
				context.write(new Text(year), new IntWritable(airTem));
			}
			
		}
	}
	
	
	public static class MiniReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, Text, IntWritable>.Context arg2)
				throws IOException, InterruptedException {
			
			int minVALUE=Integer.MAX_VALUE;
			for(IntWritable value:arg1){
				minVALUE=Math.min(minVALUE, Integer.parseInt(value.toString()));
			}
			arg2.write(arg0,new IntWritable(minVALUE));
		}
		
	}
	
	
	
	public static void main(String[] args) throws Exception, IOException {
		if(args.length != 2) {
            System.err.println("Usage: MinTemperature<input path> <output path>");
            System.exit(-1);
        }
       
        Job job = new Job();
        job.setJarByClass(MiniTemp.class);
        job.setJobName("Min temperature");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MiniMapper.class);
        job.setReducerClass(MiniReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		
		
	}
	
}
