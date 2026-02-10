package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<Object, Text, Text, DoubleWritable> {

	private DoubleWritable delay = new DoubleWritable();
	private Text airport = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String currLine = value.toString();
		if (currLine.startsWith("YEAR")){
			return;
		}
		String[] lineParts = currLine.split(",");

		if (lineParts.length > 11 && !lineParts[11].isEmpty()) {
			airport.set(lineParts[4]);
			delay.set(Double.parseDouble(lineParts[11]));
			context.write(airport, delay);
		}
	}
}
