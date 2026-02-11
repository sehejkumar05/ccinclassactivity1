package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<Object, Text, Text, DoubleWritable> {

	private DoubleWritable counter = new DoubleWritable();
	private Text word = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			
		//code changed for task 1 and task 2
		String currLine = value.toString(); //convert text to string
		if (currLine.startsWith("YEAR")){ //skip header row
			return;
		}
		String[] lineParts = currLine.split(","); //split line by commas

		//make sure enough parts to get delay and delay valid
		if (lineParts.length > 11 && !lineParts[11].isEmpty()) { 
			word.set(lineParts[4]); //get the airline
			counter.set(Double.parseDouble(lineParts[11]));
			context.write(word, counter); //write the airline, delay
		}
		
		//Task 1 code
		// if (lineParts.length > 7){ //make sure enough fields
		// 	word.set(lineParts[7]); //get origin airport
		// 	context.write(word, counter);
		// }
	}
}
