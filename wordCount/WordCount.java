package wordCount;

import java.io.IOException;

import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;


public class WordCount {


  public static class TokenizerMapper 

       extends Mapper<Object, Text, Text, IntWritable>{

    

    private final static IntWritable one = new IntWritable(1);

    private Text word = new Text();

      

    public void map(Object key, Text value, Context context

                    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {

        word.set(itr.nextToken());

        context.write(word, one);

      }

    }

  }

  

  public static class IntSumReducer 

       extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();


    public void reduce(Text key, Iterable<IntWritable> values, 

                       Context context

                       ) throws IOException, InterruptedException {

      int sum = 0;

      for (IntWritable val : values) {

        sum += val.get();

      }

      result.set(sum);

      context.write(key, result);

    }

  }


  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {

      System.err.println("Usage: wordcount <in> <out>");

      System.exit(2);

    }

    Job job = new Job(conf, "word count"); //指定任務名

    job.setJarByClass(WordCount.class); //指定Class

    job.setMapperClass(TokenizerMapper.class);//調用上面的map類

    job.setCombinerClass(IntSumReducer.class);

    job.setReducerClass(IntSumReducer.class);//調用上面的reduce類

    job.setOutputKeyClass(Text.class); //指定輸出key的格式

    job.setOutputValueClass(IntWritable.class); //指定輸出value的格式

    FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //文件輸入路徑

    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); //文件輸出

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

}