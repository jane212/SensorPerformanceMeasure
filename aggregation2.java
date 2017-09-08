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

/*************THIS JAR AGGREGATE DATA INTO TOTAL COUNT, AVG SPEED, AVG OCCUPANCY AND VC COUNT*******************/

public class aggregation2 extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new aggregation2(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "Tingting/output/operational";    // Input
		String output1 = "Tingting/output/agg2";       // Round one output

	
		
		//int reduce_tasks = 5;  
		
		Configuration conf = new Configuration();
		
		Job job_one = new Job(conf, "Exp2 Program Round One"); 	
		

		job_one.setJarByClass(aggregation2.class); 
		
		//job_one.setNumReduceTasks(reduce_tasks);		
		//job_one.setMapOutputKeyClass(Text.class); 
		//job_one.setMapOutputValueClass(Text.class); 
		
		job_one.setOutputKeyClass(NullWritable.class);   
		job_one.setOutputValueClass(Text.class);  
		

		job_one.setMapperClass(Map_One.class); 
		//job_one.setReducerClass(Reduce_One.class);

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
	public static class Map_One extends Mapper<LongWritable, Text, NullWritable, Text>  {		
	
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {

			String line = value.toString();
			String[] keys = line.split("	");
			String[] nodes = keys[1].split(",");
			
			//trim names
			nodes[0] = nodes[0].trim();
		
			//total count and speed
			int speedweightsum = 0;
			int speedsum=0;
			int countsum = 0;
			int occsum=0;
			int numlanes = Integer.parseInt(nodes[5]);
			double avgocc=0.0;
			
				for(int i=0;i<=numlanes-1;i++) 
				{
					
					String count = nodes[i*11+7];
					if(count.equals("null"))
					{
						count = "0";
					}
					
					String speed = nodes[i*11+10];
					if(speed.equals("null"))
					{
						speed = "0";
					}
					
					String occ = nodes[i*11+9];
					if(occ.equals("null"))
					{
						occ = "0";
					}
					//in order to test all null and keep that record correct
					speedsum += Integer.parseInt(speed);
					occsum += Integer.parseInt(occ);
					
					avgocc=occsum/numlanes;
					
					//exclude any zero in any lane, use the rest part to calculate total count and average speed
					if(count.equals("0") | speed.equals("0"))
					{}
					else
					{
						countsum += Integer.parseInt(count);	
						speedweightsum += Integer.parseInt(count)*Integer.parseInt(speed);
					}

					
					
				}// end for
				
										
			//average speed
				double avgspeed=0.0; //indicate a whole WRONG record, error in all lanes
				
				if (countsum!=0)
				{
					avgspeed = speedweightsum/countsum/1.6;
				}
				else
				{
					if (speedsum==0)
				
					{
						countsum=99999;
						avgspeed=99999.99; //indicates a whole RIGHT record, all null records in each lane, no any vehicle in that 20s
					}
				}
				
			//VC count
				int vc1countsum = 0;
				int vc2countsum = 0;
				int vc3countsum = 0;
				
				for(int i=0;i<=numlanes-1;i++)
				{
					
					String vc1count = nodes[i*11+11];
					String vc2count = nodes[i*11+13];
					String vc3count = nodes[i*11+15];
					
					if(vc1count.equals("null"))
					{
						vc1count="0";
					}
					if(vc2count.equals("null"))
					{
						vc2count="0";
					}
					
					if(vc3count.equals("null"))
					{
						vc3count="0";
					}
					
									
					vc1countsum += Integer.parseInt(vc1count);	
					vc2countsum += Integer.parseInt(vc2count);
					vc3countsum += Integer.parseInt(vc3count);
				
				}
			
				
				String scountsum = Integer.toString(countsum);
				String savgspeed = Double.toString(avgspeed);
				String savgocc = Double.toString(avgocc);
				String svc1countsum = Integer.toString(vc1countsum);
				String svc2countsum = Integer.toString(vc2countsum);
				String svc3countsum = Integer.toString(vc3countsum);
				if(vc1countsum==0)
				{
					svc1countsum="null";
				}
				if(vc2countsum==0)
				{
					svc2countsum="null";
				}
				if(vc3countsum==0)
				{
					svc3countsum="null";
				}
				
		//schema: name, date, time, number of lanes, total count, average occ, average speed, vc1-3 count.
				context.write(NullWritable.get(), new Text(nodes[0]+","+nodes[1]+","+nodes[2]+","+nodes[5]+","
				+scountsum+","+savgocc+","+savgspeed+","+svc1countsum+","+svc2countsum+","+svc3countsum));
				
				
			
									
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The first reduce class	
	public static class Reduce_One extends Reducer<NullWritable, Text, Text, Text>  {		
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fashion.
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
						
			for (Text val : values) {
				
			context.write(key, val);		
			
			}
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
 	
}