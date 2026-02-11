package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Iterator;


//changed for task 2
public class TopKReducer extends  Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private PriorityQueue<WordAndCount> pq = new PriorityQueue<WordAndCount>(10);;


    private Logger logger = Logger.getLogger(TopKReducer.class);


//    public void setup(Context context) {
//
//        pq = new PriorityQueue<WordAndCount>(10);
//    }


    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * @param text
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
//changed parameter to DoubleWriteable for task 2
   public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
           throws IOException, InterruptedException {


       // A local counter just to illustrate the number of values here!
        int counter = 0 ;


       // size of values is 1 because key only has one distinct value
       //original code removed and new code added for task 2
       for (DoubleWritable value : values) {
        pq.add(new WordAndCount(new Text(key), new DoubleWritable(value.get())));
        if (pq.size() > 3) {
            pq.poll();
        }
        }
        //code commented out was needed for task 1 but not for task 2
        // keep the priorityQueue size <= heapSize
        //    while (pq.size() > 3) { //changed 10 to 3 for task 1
        //        pq.poll();
        //    }


    }


    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("TopKReducer cleanup cleanup.");
        logger.info("pq.size() is " + pq.size());

        List<WordAndCount> values = new ArrayList<WordAndCount>(10);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());


        // reverse so they are ordered in descending order
        Collections.reverse(values);


        for (WordAndCount value : values) {
            context.write(value.getWord(), value.getCount());
            logger.info("TopKReducer - Top-10 Words are:  " + value.getWord() + "  Count:"+ value.getCount());
        }


    }

}