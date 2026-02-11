package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//changed all int writeable to doublewriteable for task 2
public class WordCountReducer extends  Reducer<Text, DoubleWritable, Text, DoubleWritable> {

   public void reduce(Text text, Iterable<DoubleWritable> values, Context context)
           throws IOException, InterruptedException {
	   
       int sum = 0;
       
       for (DoubleWritable value : values) {
           sum += value.get();
       }
       
       context.write(text, new DoubleWritable(sum));
   }
}