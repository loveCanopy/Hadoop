package MapreduceHbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceHbase extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{

	private IntWritable result=new IntWritable();
	@Override
	public void reduce(
			Text arg0,
			Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, ImmutableBytesWritable, Writable>.Context arg2)
			throws IOException, InterruptedException {
		
		int sum=0;
		for(IntWritable val:arg1){
			sum+=val.get();
		}
		result.set(sum);
		Put put =new Put(arg0.getBytes()); //ÐÐ¼ü
		put.add("content".getBytes(), "count".getBytes(), String.valueOf(sum).getBytes());
		arg2.write(new ImmutableBytesWritable(arg0.getBytes()), put);
	}
	
	
	
}
