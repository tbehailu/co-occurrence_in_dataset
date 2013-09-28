import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * This is the skeleton for CS61c project 1, Fall 2013.
 *
 * Reminder:  DO NOT SHARE CODE OR ALLOW ANOTHER STUDENT TO READ YOURS.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 *
 */
public class Proj1{

    /*
     * Inputs is a set of (docID, document contents) pairs.
     */
    public static class Map1 extends Mapper<WritableComparable, Text, Text, DoublePair> {
        /** Regex pattern to find words (alphanumeric + _). */
        final static Pattern WORD_PATTERN = Pattern.compile("\\w+");

        private String targetGram = null;
        private int funcNum = 0;

        /*
         * Setup gets called exactly once for each mapper, before map() gets called the first time.
         * It's a good place to do configuration or setup that can be shared across many calls to map
         */
        @Override
            public void setup(Context context) {
                targetGram = context.getConfiguration().get("targetWord").toLowerCase();
                try {
                    funcNum = Integer.parseInt(context.getConfiguration().get("funcNum"));
                } catch (NumberFormatException e) {
                    /* Do nothing. */
                }
            }

        @Override
            public void map(WritableComparable docID, Text docContents, Context context)
            throws IOException, InterruptedException {
                Matcher matcher = WORD_PATTERN.matcher(docContents.toString());
                Func func = funcFromNum(funcNum);

                ArrayList<Double> IdxArray = new ArrayList(); // store indices of target word
                
                double curIdx = 0;
                while (matcher.find()){
                	String word = matcher.group();
                    Text curWord = new Text(word.toLowerCase()); 
                    if (curWord.toString().equals(targetGram)){
                        IdxArray.add(curIdx);
                    }
                    curIdx++;
                }
                

                /**** loop through non-target words ***/
                
                matcher = WORD_PATTERN.matcher(docContents.toString()); // reset matcher
                double Idx2 = 0; // word counter in document
                double curDistance;
                double Distance;
                Text curWord;
                while (matcher.find()) {
                    curWord = new Text(matcher.group().toLowerCase());
                    Distance = Double.POSITIVE_INFINITY; // largest number represented
                    curDistance = 0;
                    if (!(curWord.toString().equals(targetGram))) { // identify non-target word
                        for (int i=0; i<IdxArray.size(); i++){ // get closest distance for word
                            curDistance = Math.abs(Idx2-IdxArray.get(i));
                            if (Distance > curDistance) {
                                Distance = curDistance; 
                            }
                        }
                        context.write(curWord, new DoublePair(Double.valueOf(1),func.f(Distance)));
                    }
                    Idx2++;
                }
                
            }

        /** Returns the Func corresponding to FUNCNUM*/
        private Func funcFromNum(int funcNum) {
            Func func = null;
            switch (funcNum) {
                case 0:	
                    func = new Func() {
                        public double f(double d) {
                            return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0;
                        }			
                    };	
                    break;
                case 1:
                    func = new Func() {
                        public double f(double d) {
                            return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0 + 1.0 / d;
                        }			
                    };
                    break;
                case 2:
                    func = new Func() {
                        public double f(double d) {
                            return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0 + Math.sqrt(d);
                        }			
                    };
                    break;
            }
            return func;
        }
    }

    /** Here's where you'll be implementing your combiner. It must be non-trivial for you to receive credit. */
    public static class Combine1 extends Reducer<Text, DoublePair, Text, DoublePair> {

        @Override
            public void reduce(Text key, Iterable<DoublePair> values,
                    Context context) throws IOException, InterruptedException {

                /**** Calculate # of occurences and sum of f(d) ****/
                double numOccurrences = 0;
                double sumFunc = 0;
                for (DoublePair word : values) {
                    numOccurrences += word.getDouble1();
                    sumFunc += word.getDouble2();
                }
                context.write(key, new DoublePair(numOccurrences, sumFunc));
            }
    }


    public static class Reduce1 extends Reducer<Text, DoublePair, DoubleWritable, Text> {
        @Override
            public void reduce(Text key, Iterable<DoublePair> values,
                    Context context) throws IOException, InterruptedException {

                /******* Same as Combiner ****/
                
                double numOccurrences = 0;
                double sumFunc = 0;
                
                for (DoublePair word : values) {
                    numOccurrences += word.getDouble1();
                    sumFunc += word.getDouble2();
                }
               
                /*****************************/
                
                double rate = 0;
                if (sumFunc > 0){
                    rate = sumFunc*(Math.pow(Math.log(sumFunc),3))/numOccurrences;
                }
                context.write(new DoubleWritable(-rate),key);
            }
    }

    public static class Map2 extends Mapper<DoubleWritable, Text, DoubleWritable, Text> {
        //maybe do something, maybe don't
    }

    public static class Reduce2 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

        int n = 0;
        static int N_TO_OUTPUT = 100;

        /*
         * Setup gets called exactly once for each reducer, before reduce() gets called the first time.
         * It's a good place to do configuration or setup that can be shared across many calls to reduce
         */
        @Override
            protected void setup(Context c) {
                n = 0;
            }

        /*
         * Your output should be a in the form of (DoubleWritable score, Text word)
         * where score is the co-occurrence value for the word. Your output should be
         * sorted from largest co-occurrence to smallest co-occurrence.
         */
        @Override
            public void reduce(DoubleWritable key, Iterable<Text> values,
                    Context context) throws IOException, InterruptedException {

                for (Text value : values) {
                    context.write(new DoubleWritable(-key.get()),value);
                }

            }
    }

    /*
     *  You shouldn't need to modify this function much. If you think you have a good reason to,
     *  you might want to discuss with staff.
     *
     *  The skeleton supports several options.
     *  if you set runJob2 to false, only the first job will run and output will be
     *  in TextFile format, instead of SequenceFile. This is intended as a debugging aid.
     *
     *  If you set combiner to false, the combiner will not run. This is also
     *  intended as a debugging aid. Turning on and off the combiner shouldn't alter
     *  your results. Since the framework doesn't make promises about when it'll
     *  invoke combiners, it's an error to assume anything about how many times
     *  values will be combined.
     */
    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        boolean runJob2 = conf.getBoolean("runJob2", true);
        boolean combiner = conf.getBoolean("combiner", false);

        System.out.println("Target word: " + conf.get("targetWord"));
        System.out.println("Function num: " + conf.get("funcNum"));

        if(runJob2)
            System.out.println("running both jobs");
        else
            System.out.println("for debugging, only running job 1");

        if(combiner)
            System.out.println("using combiner");
        else
            System.out.println("NOT using combiner");

        Path inputPath = new Path(args[0]);
        Path middleOut = new Path(args[1]);
        Path finalOut = new Path(args[2]);
        FileSystem hdfs = middleOut.getFileSystem(conf);
        int reduceCount = conf.getInt("reduces", 32);

        if(hdfs.exists(middleOut)) {
            System.err.println("can't run: " + middleOut.toUri().toString() + " already exists");
            System.exit(1);
        }
        if(finalOut.getFileSystem(conf).exists(finalOut) ) {
            System.err.println("can't run: " + finalOut.toUri().toString() + " already exists");
            System.exit(1);
        }

        {
            Job firstJob = new Job(conf, "job1");

            firstJob.setJarByClass(Map1.class);

            /* You may need to change things here */
            firstJob.setMapOutputKeyClass(Text.class);
            firstJob.setMapOutputValueClass(DoublePair.class);
            firstJob.setOutputKeyClass(DoubleWritable.class);
            firstJob.setOutputValueClass(Text.class);
            /* End region where we expect you to perhaps need to change things. */

            firstJob.setMapperClass(Map1.class);
            firstJob.setReducerClass(Reduce1.class);
            firstJob.setNumReduceTasks(reduceCount);


            if(combiner)
                firstJob.setCombinerClass(Combine1.class);

            firstJob.setInputFormatClass(SequenceFileInputFormat.class);
            if(runJob2)
                firstJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(firstJob, inputPath);
            FileOutputFormat.setOutputPath(firstJob, middleOut);

            firstJob.waitForCompletion(true);
        }

        if(runJob2) {
            Job secondJob = new Job(conf, "job2");

            secondJob.setJarByClass(Map1.class);
            /* You may need to change things here */
            secondJob.setMapOutputKeyClass(DoubleWritable.class);
            secondJob.setMapOutputValueClass(Text.class);
            secondJob.setOutputKeyClass(DoubleWritable.class);
            secondJob.setOutputValueClass(Text.class);
            /* End region where we expect you to perhaps need to change things. */

            secondJob.setMapperClass(Map2.class);
            secondJob.setReducerClass(Reduce2.class);

            secondJob.setInputFormatClass(SequenceFileInputFormat.class);
            secondJob.setOutputFormatClass(TextOutputFormat.class);
            secondJob.setNumReduceTasks(1);


            FileInputFormat.addInputPath(secondJob, middleOut);
            FileOutputFormat.setOutputPath(secondJob, finalOut);

            secondJob.waitForCompletion(true);
        }
    }

}
