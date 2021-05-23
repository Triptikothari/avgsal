import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//mapper and reducer 
public class averagesalary
{
	
    //3 source code file 
   // mapper ke liye static we dont want to create instance of the class from 9 line
   public static class mapclass extends Mapper<LongWritable, Text, Text, IntWritable>
   {
       IntWritable outvalue = new IntWritable();
	   Text outkey = new Text();
	   
	  //map ki method it read the input and tokenizer the file and split all the individual file 
	   public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException
	    {
			
          // array variable to store the variable spliting the text into the individual words and now spliting the values of one line
          String[] column = value.toString().split(",");
		  
		  //work vala variable 
		  outkey.set(column[2]);
		  
		  //salary wala variable
		  outvalue.set(Integer.parseInt(column[3]));
		  
		  //dono variable ki value context me laayi
		  context.write(outkey,outvalue);  
		   
	    }//void map kaa
		
	} //tokenizer ka hai yeah 
	
	//reducer code  it receive all the values that have the same key as the input and output the key and the number of the occurences
	public static class reducerclass extends Reducer<Text, IntWritable, Text, IntWritable>
	{
	  IntWritable outvalue= new IntWritable();
	
	  //reducer ki class
	  public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException
	  {
	//values is occuring count of same value
	   int sum=0;int avgsal;int count=0;
	   for (IntWritable sal:values)
	   {
	   sum= sum+sal.get();
	   count++;
	   }
	   avgsal=sum/count;
	   outvalue.set(avgsal);
	   context.write(key, outvalue); 
	   
	  }//reducer ki class kahtam
	}
	
	
	// ab in dono mapper and reducer ko call karungi it will be almost same 
	
	public static void main(String[] args) throws Exception{
	Configuration conf= new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
	
	  //otherArgs ko karenge checking the arguments are passing it is for input and output file 
	  if(otherArgs.length != 2){
	  System.err.println("Usage: averagesalary <in> <out>");
	  System.exit(2);
	  }
	
	  Job job = Job.getInstance(conf, "average salary");
	  job.setJar("averagesalary.jar");
	  
	  //class used in code
	  job.setJarByClass(averagesalary.class);
	  job.setMapperClass(mapclass.class);
	  job.setReducerClass(reducerclass.class);
	  
	  
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(IntWritable.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(IntWritable.class);
	 
	  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	  System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
	
	
	
	
	
	
	
	
	
	












	