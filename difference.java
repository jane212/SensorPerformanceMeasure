import java.io.*;
import java.lang.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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



/*************THIS JAR ONLY CALCULATE THE DIFFERENCE AND TAKE TWO-WEEK AVERAGE, DAILY MAXIMUM*******************/

public class difference extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new difference(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "Tingting/output/match1";    // Input
		String output1 = "Tingting/output/difference/all";       // Round one output
		String output2 = "Tingting/output/difference/average";     // Round two output 
		
	
		
		//int reduce_tasks = 5;  
		Configuration conf = new Configuration();
		
				
		// Create the job
		Job job_one = new Job(conf, "Exp2 Program Round One"); 	
		
		// Attach the job to this Driver
		job_one.setJarByClass(difference.class); 
		
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
		job_two.setJarByClass(difference.class); 
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
	//is double test, return true or false
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
			

			String line = value.toString();
			
			// Split the edge into two nodes 
			String[] nodes = line.split("	");
			
			
			//see if all names are match with order by checking the length
			//ignore all other sensors not in the list
			int length=nodes.length;
			 
			int time=Integer.parseInt(nodes[2]);
			
			if (length>9) {

					if(isInteger(nodes[11])==true && nodes[12].equals("no") && (time>220000 || time<40000))
					{
						//key: route,direction,date,time; value:name,ramp,order,count,speed, vc1-3.
						context.write(new Text(nodes[9]+","+nodes[10]+","+nodes[1]+","+nodes[2]), 
						new Text(nodes[0]+","+nodes[12]+","+nodes[11]+","+nodes[4]+","+nodes[5]+","+nodes[6]+","+nodes[7]+","+nodes[8]));	
					}
				
			}					
		} // End method "map"
		
	} // End Class Map_One
	
	
		// The reduce class	
		public static class Reduce_One extends Reducer<Text, Text, NullWritable, Text>  {		
		
			//reduce method
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
						String Tvalue2 = Torder.get(i); //current detector data
						String newTvalue2 = Tvalue2 +","+"null"+","+"null"+","+"null"+","+"null"+","+"null"; //for difference use
				
						if(Torder.containsKey(i-1)==true)
						{
					
							String Tvalue1 = Torder.get(i-1);//last detector data
							
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
						
						
						//key: (); value: route,direction,date,starttime,name,ramp,order,count,speed, vc1-3, cd, sd, vcd1-3.
						context.write(NullWritable.get(), new Text(key.toString()+","+newTvalue2));
						
					}//end if for current null key
		
			
				}//end for
			
		
		} // End method "reduce" 
		
	} // End Reduce_One class
	
	
	
	
	
	
	// The Round Two Mapper
		public static class Map_Two extends Mapper<LongWritable, Text, Text, Text>  {		
		
					
 		public void map(LongWritable key, Text value, Context context) 
 				throws IOException, InterruptedException  { 			
 	
			String[] nodes = value.toString().split(",");			
			
				//key: name; value: route, direction, order, ramp, cd, sd, vcd1-3.
				context.write(new Text(nodes[4]), 
						new Text(nodes[0]+","+nodes[1]+","+nodes[6]+","+nodes[5]+","+nodes[12]+","+nodes[13]+","+nodes[14]+","+nodes[15]+","+nodes[16]));	
				
			
 		}  // End method "map"
 		
 	}  // End Class Map_Two
		
 	
 	// The second Reduce class
 	public static class Reduce_Two extends Reducer<Text, Text, NullWritable, Text>  { 		
 				
 		public void reduce(Text key, Iterable<Text> values, Context context) 
 				throws IOException, InterruptedException  { 			
 			
 			long cd=0;
 			double sd=0;
 			long vcd1=0;
 			long vcd2=0;
 			long vcd3=0;
 			//all records including null
 			int countall=0;
 			//records not including null, for cd, sd, vcd1-3
 			int countcd=0;
 			int countsd=0;
 			int countvcd1=0;	
 			int countvcd2=0;
 			int countvcd3=0;
 			
 			String route="";
 			String direction="";
 			String order="";
 			String ramp="";
 			
 			for(Text val:values){
 			 	String line=val.toString();
 				String[] nodes=line.split(",");
 				route=nodes[0];
 				direction=nodes[1];
 				order=nodes[2];
 				ramp=nodes[3];
 				break;
 			}
 			
 			//get total
 			for(Text val:values){
 				
 				countall++;
 				
 				String line=val.toString();
 				String[] nodes=line.split(",");
 				
 				//cd
 				if(nodes[4].equals("null")){
 				}	
 				else{
 					
 					cd=cd+Integer.parseInt(nodes[4]);
 					countcd++;
 				}
 				
 				//sd
 				if(nodes[5].equals("null")){
 				}	
 				else{
 					
 					sd=sd+Double.parseDouble(nodes[5]);
 					countsd++;
 				}
 				
 				//vcd1
 				if(nodes[6].equals("null")){
 				}	
 				else{
 					
 					vcd1=vcd1+Integer.parseInt(nodes[6]);
 					countvcd1++;
 				}
 				
 				//vcd2
 				if(nodes[7].equals("null")){
 				}	
 				else{
 					
 					vcd2=vcd2+Integer.parseInt(nodes[7]);
 					countvcd2++;
 				}
 				 				
 				//vcd3
 				if(nodes[8].equals("null")){
 				}	
 				else{
 					
 					vcd3=vcd3+Integer.parseInt(nodes[8]);
 					countvcd3++;
 				}
 				
 			} //End for
 			
 			//get average
 			String avgcd="null";
 			if(countcd!=0){
 				avgcd=Long.toString(cd/countcd);
 			}
 			
 			String avgsd="null";
 			if(countsd!=0){
 				avgsd=Double.toString(sd/countsd);
 			}
 			
 			String avgvcd1="null";
 			if(countvcd1!=0){
 				avgvcd1=Long.toString(vcd1/countvcd1);
 			}
 			
 			String avgvcd2="null";
 			if(countvcd2!=0){
 				avgvcd2=Long.toString(vcd2/countvcd2);
 			}
 			
 			String avgvcd3="null";
 			if(countvcd3!=0){
 				avgvcd3=Long.toString(vcd3/countvcd3);
 			}
 			
 			
 				context.write(NullWritable.get(), new Text(key.toString()+","+route+","+direction+","+order+","+ramp+","+avgcd+","+avgsd+","
 								+avgvcd1+","+avgvcd2+","+avgvcd3+","+Integer.toString(countall)+","+Integer.toString(countcd)+","+Integer.toString(countsd)
 								+","+Integer.toString(countvcd1)+","+Integer.toString(countvcd2)+","+Integer.toString(countvcd3)));	
 			
 			
		
		}  // End method "reduce"
		
	}  // End Class Reduce_Two
 
}
