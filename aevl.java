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

/*************THIS JAR AGGREGATE DATA INTO TOTAL COUNT, AVG SPEED AND VC COUNT*******************/

public class aevl extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new aevl(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "Tingting/output/agg2";    // Input
		String output1 = "Tingting/output/aevl";       // Round one output

	
		
		//int reduce_tasks = 5;  
		
		Configuration conf = new Configuration();
		
		Job job_one = new Job(conf, "aevlcheck"); 	
		

		job_one.setJarByClass(aevl.class); 
		
		//job_one.setNumReduceTasks(reduce_tasks);		
		job_one.setMapOutputKeyClass(Text.class); 
		job_one.setMapOutputValueClass(Text.class); 
		
		job_one.setOutputKeyClass(NullWritable.class);   
		job_one.setOutputValueClass(Text.class);  
		

		job_one.setMapperClass(Map_One.class); 
		job_one.setReducerClass(Reduce_One.class);

		job_one.setInputFormatClass(TextInputFormat.class);  

		job_one.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_one, new Path(input)); 
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path)); // This is legal

		FileOutputFormat.setOutputPath(job_one, new Path(output1));
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); // This is not allowed
	
		job_one.waitForCompletion(true); 
		
		return 0;
	
	} // End run

	
	// The round one
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {		
	
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {

			String line = value.toString();
			String[] nodes = line.split(",");
			//trim name
			nodes[0]=nodes[0].trim();
		
			int count=Integer.parseInt(nodes[4]);
			double occ=Double.parseDouble(nodes[5]);
			double speed=Double.parseDouble(nodes[6]);
			int cserror=0;
			int vlerror=0;
			
			//realistic check			
			if ((count==0 & speed>0) | (count>0 & speed==0)){
					cserror=1;
			}
			
			else{
				//aevl
				//vl=(5280*speed*occ)/(100*count*12),vl within(10:75)ft
					double vl=(5280*speed*occ)/(100*count*180);
				
					if (vl>75 | vl<10){
						vlerror=1;
					}
			}
			
		//schema: key: name, value: cs error, vl error.
			context.write(new Text(nodes[0]), 
							new Text(Integer.toString(cserror)+","+Integer.toString(vlerror)));
				
				
			
									
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The first reduce class	
	public static class Reduce_One extends Reducer<Text, Text, NullWritable, Text>  {		
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fashion.
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
			int cserror=0;
			int vlerror=0;
			double total=0;
			for (Text val : values) {
				
				total++;
				
				String line=val.toString();
				String[] nodes =line.split(",");
				cserror=cserror+Integer.parseInt(nodes[0]);
				vlerror=vlerror+Integer.parseInt(nodes[1]);
				
			}
			int itotal=(int)total;
			
			//schema: name, wz name, number of lanes, dir, upstream, downstream, number of records, cs error %, vl error %.
			context.write(NullWritable.get(), 
					new Text(key.toString()+","+Integer.toString(itotal)+","
							+Double.toString(Math.round(vlerror/total*100000.0)/1000.0)+"%"));	
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
 	
}