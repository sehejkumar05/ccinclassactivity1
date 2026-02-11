package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;


public class WordAndCount implements Comparable<WordAndCount> {

        private final Text word;
        private final DoubleWritable count; //changed for task 2
        

        public WordAndCount(Text word, DoubleWritable count) { //parameter changed for task 2
            this.word = word;
            this.count = count;
        }

        public Text getWord() {
            return word;
        }

        public DoubleWritable getCount() {
            return count;
        }
    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
        //compare method changed for task 2
        @Override
        public int compareTo(WordAndCount other) {
            return Double.compare(this.count.get(), other.count.get());
        }


        public String toString(){

            return "("+word.toString() +" , "+ count.toString()+")";
        }
    }

