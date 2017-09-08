import java.io.*;
import java.lang.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



/*************THIS JAR GET MAX VALUE FOR EACH DETECTOR EACH DAY*******************/

public class daily extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new daily(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "Tingting/output/difference/all";    // Input
		String output1 = "Tingting/output/dailymax/cd";       // Round one output
		String output2 = "Tingting/output/dailymax/sd";     // Round two output 
		String output3 = "Tingting/output/dailymax/vcd1";   // Round three output 
		String output4 = "Tingting/output/dailymax/vcd2";   // Round three output 
		String output5 = "Tingting/output/dailymax/vcd3";   // Round three output 
	
		
		//int reduce_tasks = 5;  
		Configuration conf = new Configuration();
		
				
		//1
		Job job_one = new Job(conf, "Exp2 Program Round One"); 	
		
		job_one.setJarByClass(daily.class); 
	
		job_one.setMapOutputKeyClass(Text.class); 
		job_one.setMapOutputValueClass(Text.class); 
		job_one.setOutputKeyClass(NullWritable.class);   
		job_one.setOutputValueClass(Text.class);  
		
		job_one.setMapperClass(Map_One.class); 
		
		job_one.setReducerClass(Reduce_One.class);
		
		job_one.setInputFormatClass(TextInputFormat.class);  
		
		job_one.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job_one, new Path(input)); 
		
		FileOutputFormat.setOutputPath(job_one, new Path(output1));
		
		job_one.waitForCompletion(true); 
		
		//2
		
		Job job_two = new Job(conf, "Exp2 Program Round One"); 	
		
		job_two.setJarByClass(daily.class); 
	
		job_two.setMapOutputKeyClass(Text.class); 
		job_two.setMapOutputValueClass(Text.class); 
		job_two.setOutputKeyClass(NullWritable.class);   
		job_two.setOutputValueClass(Text.class);  
		
		job_two.setMapperClass(Map_Two.class); 
		
		job_two.setReducerClass(Reduce_Two.class);
		
		job_two.setInputFormatClass(TextInputFormat.class);  
		
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job_two, new Path(input)); 
		
		FileOutputFormat.setOutputPath(job_two, new Path(output2));
		
		job_two.waitForCompletion(true); 
		
	//3
		Job job_three = new Job(conf, "Exp2 Program Round One"); 	
		
		job_three.setJarByClass(daily.class); 
	
		job_three.setMapOutputKeyClass(Text.class); 
		job_three.setMapOutputValueClass(Text.class); 
		job_three.setOutputKeyClass(NullWritable.class);   
		job_three.setOutputValueClass(Text.class);  
		
		job_three.setMapperClass(Map_Three.class); 
		
		job_three.setReducerClass(Reduce_Three.class);
		
		job_three.setInputFormatClass(TextInputFormat.class);  
		
		job_three.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job_three, new Path(input)); 
		
		FileOutputFormat.setOutputPath(job_three, new Path(output3));
		
		job_three.waitForCompletion(true); 
		
		//4
	Job job_four = new Job(conf, "Exp2 Program Round One"); 	
		
		job_four.setJarByClass(daily.class); 
	
		job_four.setMapOutputKeyClass(Text.class); 
		job_four.setMapOutputValueClass(Text.class); 
		job_four.setOutputKeyClass(NullWritable.class);   
		job_four.setOutputValueClass(Text.class);  
		
		job_four.setMapperClass(Map_Four.class); 
		
		job_four.setReducerClass(Reduce_Four.class);
		
		job_four.setInputFormatClass(TextInputFormat.class);  
		
		job_four.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job_four, new Path(input)); 
		
		FileOutputFormat.setOutputPath(job_four, new Path(output4));
		
		job_four.waitForCompletion(true); 
		
		//5
	Job job_five = new Job(conf, "Exp2 Program Round One"); 	
		
		job_five.setJarByClass(daily.class); 
	
		job_five.setMapOutputKeyClass(Text.class); 
		job_five.setMapOutputValueClass(Text.class); 
		job_five.setOutputKeyClass(NullWritable.class);   
		job_five.setOutputValueClass(Text.class);  
		
		job_five.setMapperClass(Map_Five.class); 
		
		job_five.setReducerClass(Reduce_Five.class);
		
		job_five.setInputFormatClass(TextInputFormat.class);  
		
		job_five.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job_five, new Path(input)); 
		
		FileOutputFormat.setOutputPath(job_five, new Path(output5));
		
		job_five.waitForCompletion(true); 
		
		return 0;
		
	
	} // End run
	
	
	// The round one
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {		
	
		
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
		
			String line = value.toString();
			
		
			String[] nodes = line.split(",");
							
			if (nodes[12].equals("null")){}
			
			else{
				context.write(new Text(nodes[4]+","+nodes[2]), value);	
			
			}		
		} 
		
	} 
	
	// The first reduce class	
	public static class Reduce_One extends Reducer<Text, Text, NullWritable, Text>  {		
		
		
		public void reduce(Text key, Iterable<Text> values, Context context) 	throws IOException, InterruptedException{
	
		TreeMap<Integer, String> Tall = new TreeMap<Integer, String>();
						
		for (Text val:values)
		{
			String[] nodes  = val.toString().split(",");
			
			int cdnum = Math.abs(Integer.parseInt(nodes[12]));
			
			
			Tall.put(cdnum, val.toString());
			
			if(Tall.size()>1){
				Tall.remove(Tall.firstKey());
			}
			
		
}
		for (String t : Tall.values()){
			
			String[] nodes=t.split(",");
			
			context.write(NullWritable.get(), new Text(nodes[4]+","+nodes[2]+","+nodes[3]+","+nodes[0]+","+nodes[1]+","+nodes[6]+","+nodes[5]+","+nodes[7]+","+nodes[12]));
		}
}
} 
	
	
	// The round two
	public static class Map_Two extends Mapper<LongWritable, Text, Text, Text>  {		
	
		
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
		
			String line = value.toString();
			
		
			String[] nodes = line.split(",");
							
			if (nodes[13].equals("null")){}
			
			else{
				context.write(new Text(nodes[4]+","+nodes[2]), value);	
			}
								
		} 
		
	} 
	
	// The reduce class	
	public static class Reduce_Two extends Reducer<Text, Text, NullWritable, Text>  {		
		
		
		public void reduce(Text key, Iterable<Text> values, Context context) 	throws IOException, InterruptedException{
	
		TreeMap<Double, String> Tall = new TreeMap<Double, String>();
						
		for (Text val:values)
		{
			String[] nodes  = val.toString().split(",");
			

			double sdnum = Math.abs(Double.parseDouble(nodes[13]));
			
			
			Tall.put(sdnum, val.toString());
			
			if(Tall.size()>1){
				Tall.remove(Tall.firstKey());
			}
			
		
}
		for (String t : Tall.values()){
			
			String[] nodes=t.split(",");
			
			context.write(NullWritable.get(), new Text(nodes[4]+","+nodes[2]+","+nodes[3]+","+nodes[0]+","+nodes[1]+","+nodes[6]+","+nodes[5]+","+nodes[8]+","+nodes[13]));
		}
}
} 
	// The round three
	public static class Map_Three extends Mapper<LongWritable, Text, Text, Text>  {		
	
		
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
		
			String line = value.toString();
			
		
			String[] nodes = line.split(",");
			
			if (nodes[14].equals("null")){}
			
			else{				
			
				context.write(new Text(nodes[4]+","+nodes[2]), value);	
			}
								
		} 
		
	} 
	
	// The first reduce class	
	public static class Reduce_Three extends Reducer<Text, Text, NullWritable, Text>  {		
		
		
		public void reduce(Text key, Iterable<Text> values, Context context) 	throws IOException, InterruptedException{
	
		TreeMap<Integer, String> Tall = new TreeMap<Integer, String>();
						
		for (Text val:values)
		{
			String[] nodes  = val.toString().split(",");
			
			int vcd1num = Math.abs(Integer.parseInt(nodes[14]));
			
			
			Tall.put(vcd1num, val.toString());
			
			if(Tall.size()>1){
				Tall.remove(Tall.firstKey());
			}
			

}
		for (String t : Tall.values()){

			String[] nodes=t.split(",");
			
			context.write(NullWritable.get(), new Text(nodes[4]+","+nodes[2]+","+nodes[3]+","+nodes[0]+","+nodes[1]+","+nodes[6]+","+nodes[5]+","+nodes[9]+","+nodes[14]));
		}
}
		
		
} 
	
	// The round four
	public static class Map_Four extends Mapper<LongWritable, Text, Text, Text>  {		
	
		
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
		
			String line = value.toString();
			
		
			String[] nodes = line.split(",");
			
			if (nodes[15].equals("null")){}
			
			else{				
			
				context.write(new Text(nodes[4]+","+nodes[2]), value);	
			}
								
		} 
		
	} 
	
	// The first reduce class	
	public static class Reduce_Four extends Reducer<Text, Text, NullWritable, Text>  {		
		
		
		public void reduce(Text key, Iterable<Text> values, Context context) 	throws IOException, InterruptedException{
	
		TreeMap<Integer, String> Tall = new TreeMap<Integer, String>();
						
		for (Text val:values)
		{
			String[] nodes  = val.toString().split(",");
			
			int vcd1num = Math.abs(Integer.parseInt(nodes[15]));
			
			
			Tall.put(vcd1num, val.toString());
			
			if(Tall.size()>1){
				Tall.remove(Tall.firstKey());
			}
			

}
		for (String t : Tall.values()){
			
			String[] nodes=t.split(",");
			
			context.write(NullWritable.get(), new Text(nodes[4]+","+nodes[2]+","+nodes[3]+","+nodes[0]+","+nodes[1]+","+nodes[6]+","+nodes[5]+","+nodes[10]+","+nodes[15]));
		}
}
} 
	// The round five
	public static class Map_Five extends Mapper<LongWritable, Text, Text, Text>  {		
	
		
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
		
			String line = value.toString();
			
		
			String[] nodes = line.split(",");
			
			if (nodes[16].equals("null")){}
			
			else{				
			
				context.write(new Text(nodes[4]+","+nodes[2]), value);	
			}
								
		} 
		
	} 
	
	// The first reduce class	
	public static class Reduce_Five extends Reducer<Text, Text, NullWritable, Text>  {		
		
		
		public void reduce(Text key, Iterable<Text> values, Context context) 	throws IOException, InterruptedException{
	
		TreeMap<Integer, String> Tall = new TreeMap<Integer, String>();
						
		for (Text val:values)
		{
			String[] nodes  = val.toString().split(",");
			
			int vcd1num = Math.abs(Integer.parseInt(nodes[16]));
			
			
			Tall.put(vcd1num, val.toString());
			
			if(Tall.size()>1){
				Tall.remove(Tall.firstKey());
			}
			

}
		for (String t : Tall.values()){

			String[] nodes=t.split(",");
			
			context.write(NullWritable.get(), new Text(nodes[4]+","+nodes[2]+","+nodes[3]+","+nodes[0]+","+nodes[1]+","+nodes[6]+","+nodes[5]+","+nodes[11]+","+nodes[16]));
		}
}
} 	
	
	
	
}