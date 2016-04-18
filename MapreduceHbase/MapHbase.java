package MapreduceHbase;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapHbase extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one=new IntWritable(1);
	private Text word=new Text();
	@Override
	public void map(Object key, Text value,
			Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer stringTokenizer=new StringTokenizer(value.toString());
		while(stringTokenizer.hasMoreTokens()){
			word.set(stringTokenizer.nextToken());
			context.write(word, one);
		}
		
	}
	
	
	
}
