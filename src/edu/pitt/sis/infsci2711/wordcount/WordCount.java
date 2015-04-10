package edu.pitt.sis.infsci2711.wordcount;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new WordCount(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "WordCount");
      job.setJarByClass(WordCount.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
         for (String token: value.toString().split("\\s+")) {
        	 if(token.length()>0&&token!=null){
        	 char first =Character.toLowerCase(token.charAt(0));
        
            switch(first){
               case 'a':
                  word.set("a");
                 context.write(word,ONE);
                 break;
                  case 'b': 
                	  word.set("b");
                 context.write(word,ONE);
                 break;
                     case 'c':
                    	 word.set("c");
                 context.write(word,ONE);
                 break;
                 case 'd':
                	 word.set("d");
                 context.write(word,ONE);
                 break;
                 case 'e': 
                	 word.set("e");
                 context.write(word,ONE);
                 break;
                 case 'f': 
                	 word.set("f");
                 context.write(word,ONE);
                 break;
                 case 'g':
                	 word.set("g");
                 context.write(word,ONE);
                 break;
                 case 'h': 
                	 word.set("h");
                 context.write(word,ONE);
                 break;
                 case 'i': 
                	 word.set("i");
                 context.write(word,ONE);
                 break;
                 case 'j':
                	 word.set("j");
                 context.write(word,ONE);
                 break;
                 case 'k': 
                	 word.set("k");
                 context.write(word,ONE);
                 break;
                 case 'l':
                     word.set("l");
                    context.write(word,ONE);
                    break;
                     case 'm': 
                   	  word.set("m");
                    context.write(word,ONE);
                    break;
                        case 'n':
                       	 word.set("n");
                    context.write(word,ONE);
                    break;
                    case 'o':
                   	 word.set("o");
                    context.write(word,ONE);
                    break;
                    case 'p': 
                   	 word.set("p");
                    context.write(word,ONE);
                    break;
                    case 'q': 
                   	 word.set("f");
                    context.write(word,ONE);
                    break;
                    case 'r':
                   	 word.set("r");
                    context.write(word,ONE);
                    break;
                    case 's': 
                   	 word.set("s");
                    context.write(word,ONE);
                    break;
                    case 't': 
                   	 word.set("t");
                    context.write(word,ONE);
                    break;
                    case 'u':
                   	 word.set("u");
                    context.write(word,ONE);
                    break;
                    case 'v': 
                   	 word.set("v");
                    context.write(word,ONE);
                    break;
                    case 'w':
                    	word.set("w");
                    	context.write(word, ONE);
                    case 'x':
                      	 word.set("x");
                       context.write(word,ONE);
                       break;
                       case 'y': 
                      	 word.set("y");
                       context.write(word,ONE);
                       break;
                       case 'z':
                       	word.set("z");
                       	context.write(word, ONE);
                    		
                 default:
                	 break;
               
              }
        	 
        	 }
            
            //word.set(token);
            //context.write(word, ONE);
         }
      }
   }

   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
         context.write(key, new IntWritable(sum));
      }
   }
}