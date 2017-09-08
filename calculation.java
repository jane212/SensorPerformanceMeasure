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



/*************THIS JAR CALCULATE THE CUMULATIVE DIFFERENCE (EXCEPT SPEED)*******************/

public class calculation extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new calculation(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "Tingting/output/match1";    // Input
		String output1 = "Tingting/output/cal1";       // Round one output
		String output2 = "Tingting/output/cal2";     // Round two output 
		
	
		
		//int reduce_tasks = 5;  
		Configuration conf = new Configuration();
		
				
		// Create the job
		Job job_one = new Job(conf, "Exp2 Program Round One"); 	
		
		// Attach the job to this Driver
		job_one.setJarByClass(calculation.class); 
		
		//job_one.setNumReduceTasks(reduce_tasks);		
		
		job_one.setMapOutputKeyClass(Text.class); 
		job_one.setMapOutputValueClass(Text.class); 
		job_one.setOutputKeyClass(NullWritable.class);   
		job_one.setOutputValueClass(Text.class);  
		
		// The class that provides the map method
		job_one.setMapperClass(Map_One.class); 
		
		// The class that provides the reduce method
		job_one.setReducerClass(Reduce_One.class);
		

		// This means each map method receives one line as an input
		job_one.setInputFormatClass(TextInputFormat.class);  
		
		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);
		
		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input)); 
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path)); // This is legal
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(output1));
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); // This is not allowed
		
		// Run the job
		job_one.waitForCompletion(true); 
		

		
		Job job_two = new Job(conf, "Driver Program Round Two"); 
		job_two.setJarByClass(calculation.class); 
		//job_two.setNumReduceTasks(reduce_tasks); 
		
		job_two.setMapOutputKeyClass(Text.class); 
		job_two.setMapOutputValueClass(Text.class); 
		job_two.setOutputKeyClass(NullWritable.class); 
		job_two.setOutputValueClass(Text.class);
		
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(Map_Two.class); 
		job_two.setReducerClass(Reduce_Two.class);
		
		job_two.setInputFormatClass(TextInputFormat.class); 
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(output1)); 
		FileOutputFormat.setOutputPath(job_two, new Path(output2));
		
		// Run the job
		job_two.waitForCompletion(true); 
		
		
		/*// Create job for round 3: Round 3 counts all the triangles in the network. Output to 'output1'
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
				
		Job job_Three = new Job(conf, "Driver Program Round Three"); 
		job_Three.setJarByClass(calculation.class); 
		//job_Three.setNumReduceTasks(1); 
				
		job_Three.setOutputKeyClass(Text.class); 
		job_Three.setOutputValueClass(Text.class);
				
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_Three.setMapperClass(Map_Three.class); 
		//job_Three.setReducerClass(Reduce_Three.class);
			
		job_Three.setInputFormatClass(TextInputFormat.class); 
		job_Three.setOutputFormatClass(TextOutputFormat.class);
				
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_Three, new Path(input)); 
		FileOutputFormat.setOutputPath(job_Three, new Path(output3));
				
		// Run the job
		job_Three.waitForCompletion(true); 		*/
		
/*		// Create job for round 4: Round 4 counts all the triplets in the network. Output to 'output2'
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
				
		Job job_Four = new Job(conf, "Driver Program Round Four"); 
		job_Four.setJarByClass(calculation.class); 
		job_Four.setNumReduceTasks(1); 
				
		job_Four.setOutputKeyClass(Text.class); 
		job_Four.setOutputValueClass(Text.class);
				
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_Four.setMapperClass(Map_Four.class); 
		job_Four.setReducerClass(Reduce_Four.class);
			
		job_Four.setInputFormatClass(TextInputFormat.class); 
		job_Four.setOutputFormatClass(TextOutputFormat.class);
				
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_Four, new Path(temp)); 
		FileOutputFormat.setOutputPath(job_Four, new Path(output2));
				
		// Run the job
		job_Four.waitForCompletion(true); 	*/
	
		return 0;
	
	} // End run
	
	//is integer test, return true or false
	public static boolean isInteger(String s) {
	    return isInteger(s,10);
	}

	public static boolean isInteger(String s, int radix) {
	    if(s.isEmpty()) return false;
	    for(int i = 0; i < s.length(); i++) {
	        if(i == 0 && s.charAt(i) == '-') {
	            if(s.length() == 1) return false;
	            else continue;
	        }
	        if(Character.digit(s.charAt(i),radix) < 0) return false;
	    }
	    return true;
	
	}
	//is integer test, return true or false
	 public static boolean isDouble(String str) {
	        try {
	            Double.parseDouble(str);
	            return true;
	        } catch (NumberFormatException e) {
	            return false;
	        }
	    }
	 
	 
	// The round one
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {		
	
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line (edge) from the input
			String line = value.toString();
			
			// Split the edge into two nodes 
			String[] nodes = line.split("	");
			//see if all names are match with order by checking the length
			//ignore all other sensors not in the list
			int length=line.length();
			
			if (length>9) {
			
			if(isInteger(nodes[11])==true)
			{
				context.write(new Text(nodes[9]+","+nodes[10]+","+nodes[1]+","+nodes[2]), 
						new Text(nodes[0]+","+nodes[12]+","+nodes[11]+","+nodes[4]+","+nodes[5]+","+nodes[6]+","+nodes[7]+","+nodes[8]));	
			}
			}
									
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The first reduce class	
	public static class Reduce_One extends Reducer<Text, Text, NullWritable, Text>  {		
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fashion.
		public void reduce(Text key, Iterable<Text> values, Context context) 	throws IOException, InterruptedException{
			
		//build treemap	
		TreeMap<Integer, String> Torder = new TreeMap<Integer, String>();
		
		//put data into treemap, order as key, all value string as treemap value
		
		for (Text val:values)
		{
			String[] nodes  = val.toString().split(",");
			
			int ordernum = Integer.parseInt(nodes[2]);
			
			Torder.put(ordernum, val.toString());
			
		}
			
		//calculate difference, skip null value
		
		for(int i=Torder.firstKey(); i<=Torder.lastKey();i++)
		{
				
			if(Torder.containsKey(i)==true)
			{
				String Tvalue2 = Torder.get(i);
				String newTvalue2 = Tvalue2 +","+"null"+","+"null"+","+"null"+","+"null"+","+"null";
				
				if(Torder.containsKey(i-1)==true)
				{
					
				String Tvalue1 = Torder.get(i-1);
							
				//count
				String SCD="null";
				if(isInteger(Tvalue2.split(",")[3])==true & isInteger(Tvalue1.split(",")[3])==true)
					{
					int CD = Integer.parseInt(Tvalue2.split(",")[3])-Integer.parseInt(Tvalue1.split(",")[3]);
					SCD = Integer.toString(CD);
					}

				
				//speed
				String SSD="null";
				if(isDouble(Tvalue2.split(",")[4])==true & isDouble(Tvalue1.split(",")[4])==true)
					{
					double SD = Double.parseDouble(Tvalue2.split(",")[4])-Double.parseDouble(Tvalue1.split(",")[4]);
					SSD = Double.toString(SD);
					}

				
				//vc1 count
				String SVCD1="null";
				if(isInteger(Tvalue2.split(",")[5])==true & isInteger(Tvalue1.split(",")[5])==true)
					{
					int VCD1 = Integer.parseInt(Tvalue2.split(",")[5])-Integer.parseInt(Tvalue1.split(",")[5]);
					SVCD1 = Integer.toString(VCD1);
					}

				
				//vc2 count
				String SVCD2="null";
				if(isInteger(Tvalue2.split(",")[6])==true & isInteger(Tvalue1.split(",")[6])==true)
					{
					int VCD2 = Integer.parseInt(Tvalue2.split(",")[6])-Integer.parseInt(Tvalue1.split(",")[6]);
					SVCD2 = Integer.toString(VCD2);
					}

					
				//vc3 count
				String SVCD3="null";
				if(isInteger(Tvalue2.split(",")[7])==true & isInteger(Tvalue1.split(",")[7])==true)
					{
					int VCD3 = Integer.parseInt(Tvalue2.split(",")[7])-Integer.parseInt(Tvalue1.split(",")[7]);
					SVCD3 = Integer.toString(VCD3);
					}

					
				newTvalue2 = Tvalue2+","+SCD+","+SSD+","+SVCD1+","+SVCD2+","+SVCD3;
					
				
				
				}//end if for last null key

				context.write(NullWritable.get(), new Text(key.toString()+","+newTvalue2));
						
				}//end if for current null key
		
			
}//end for
			
		
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
	
	
	
	
	
	// The Round Two Mapper
		public static class Map_Two extends Mapper<LongWritable, Text, Text, Text>  {		
		
					
 		public void map(LongWritable key, Text value, Context context) 
 				throws IOException, InterruptedException  { 			
 	
			String[] nodes = value.toString().split(",");			
			

				context.write(new Text(nodes[0]+","+nodes[1]+","+nodes[6]+","+nodes[2]), 
						new Text(nodes[3]+","+nodes[4]+","+nodes[5]+","+nodes[7]+","+nodes[8]
								+","+nodes[9]+","+nodes[10]+","+nodes[11]+","+nodes[12]
								+","+nodes[13]+","+nodes[14]+","+nodes[15]+","+nodes[16]));	
				
			
 		}  // End method "map"
 		
 	}  // End Class Map_Two
		
 	
 	// The second Reduce class
 	public static class Reduce_Two extends Reducer<Text, Text, NullWritable, Text>  { 		
 				
 		public void reduce(Text key, Iterable<Text> values, Context context) 
 				throws IOException, InterruptedException  { 			
 			
			
		//build treemap	
		TreeMap<Integer, String> Ttime = new TreeMap<Integer, String>();
		
		//put data into treemap, time as key, all value string as treemap value
		
		for (Text val:values)
		{
			String[] nodes  = val.toString().split(",");
			
			int timenum = Integer.parseInt(nodes[0]);
			
			Ttime.put(timenum, val.toString());
			
		}
				
		//calculate cumulative difference
		int CCD=0;
		int CVCD1=0;
		int CVCD2=0;
		int CVCD3=0;
		
			for (int k:Ttime.keySet())	
			{
				String TTvalue2 = Ttime.get(k);
				String cd = TTvalue2.split(",")[8];
				String vcd1 = TTvalue2.split(",")[10];
				String vcd2 = TTvalue2.split(",")[11];
				String vcd3 = TTvalue2.split(",")[12];
				
				if(k==Ttime.firstKey())
				{

				}
				
				else
				//cumulative CD,VCD1,VCD2,VCD3
				{
					if(cd.equals("null")){
						cd="0";
					}
					if(vcd1.equals("null")){
						vcd1="0";
					}
					if(vcd2.equals("null")){
						vcd2="0";
					}
					if(vcd3.equals("null")){
						vcd3="0";
					}
													
				CCD += Integer.parseInt(cd);
				
				CVCD1 += Integer.parseInt(vcd1);
				
				CVCD2 += Integer.parseInt(vcd2);
				
				CVCD3 += Integer.parseInt(vcd3);
				}
					
							

				context.write(NullWritable.get(), 
						new Text(key.toString()+","+TTvalue2 +","+Integer.toString(CCD)+","+Integer.toString(CVCD1)+","+Integer.toString(CVCD2)+","+Integer.toString(CVCD3)));
						

		
 	
 }//end for		
		}  // End method "reduce"
		
	}  // End Class Reduce_Two
 
}