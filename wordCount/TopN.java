import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class TopN {

	
	
	
	
	public class TopNMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
	    int len;
	    int top[];
	    @Override
	    public void setup(Context context) throws IOException,InterruptedException {
	        len = context.getConfiguration().getInt("N", 10);
	        top = new int[len+1];
	    }
	 
	    @Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
	    String line = value.toString();
	    String arr []= line.split(",");
	    if(arr != null && arr.length == 4){
	        int pay = Integer.parseInt(arr[2]);
	        add(pay);
	    }
	}


	public void add(int pay){
	    top[0] = pay;
	    Arrays.sort(top);
	}
	 
	@Override
	public void cleanup(Context context) throws IOException,InterruptedException {
	    for(int i=1;i<=len;i++){
	        context.write(new IntWritable(top[i]),new IntWritable(top[i]));
	    }
	 }
	 
	}

	 
	public class TopNReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		int len;
		int top[];
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			len = context.getConfiguration().getInt("N", 10);
			top = new int[len+1];
		}
		
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			for(IntWritable val : values){
				add(val.get());
			}
		}
		
		public void add(int pay){
			top[0] = pay;
			Arrays.sort(top);
		}
		
		@Override
		public void cleanup(Context context)
				throws IOException, InterruptedException {
			for(int i=len;i>0;i--){
				context.write(new IntWritable(len-i+1),new IntWritable(top[i]));
			}
		}
	}
	 
	 

	 public static void main(String[] args) throws Exception {
			
    	 Configuration conf = new Configuration();

    	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    	    if (otherArgs.length != 2) {

    	      System.err.println("Usage: wordcount <in> <out>");

    	      System.exit(2);

    	    }

    	    Job job = new Job(conf, "TOPn"); //指定任務名

    	    job.setJarByClass(MTjoin.class); //指定Class

    	    job.setMapperClass(TopNMapper.class);//調用上面的map類


    	    job.setReducerClass(TopNReduce.class);//調用上面的reduce類

    	    job.setOutputKeyClass(Text.class); //指定輸出key的格式

    	    job.setOutputValueClass(IntWritable.class); //指定輸出value的格式

    	    FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //文件輸入路徑

    	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); //文件輸出

    	    System.exit(job.waitForCompletion(true) ? 0 : 1);

    	
    	
    	
	}
	  
	 
	
	
	
	
	
}
