package assignment;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class Assign1PhotoLoc {

  public static class PhotoLocKey implements WritableComparable<PhotoLocKey> {
		private String userID;
		private String datetime;
		
		/**
		 * Constructor.
		 */
		public PhotoLocKey() { }
		
		/**
		 * Constructor.
		 * @param userID Stock userID. i.e. APPL
		 * @param datetime datetime. i.e. the number of milliseconds since January 1, 1970, 00:00:00 GMT
		 */
		public PhotoLocKey(String userID, String datetime) {
			this.userID = userID;
			this.datetime = datetime;
		}
		
		@Override
		public String toString() {
			return (new StringBuilder())
					.append('{')
					.append(userID)
					.append(',')
					.append(datetime)
					.append('}')
					.toString();
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			userID = WritableUtils.readString(in);
			datetime = WritableUtils.readString(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, userID);
			WritableUtils.writeString(out, datetime);
		}

		public int compareTo(PhotoLocKey o) {
			int result = userID.compareTo(o.userID);
			if(0 == result) {
				result = datetime.compareTo(o.datetime);
			}
			return result;
		}

		/**
		 * Gets the userID.
		 * @return userID.
		 */
		public String getuserID() {
			return userID;
		}

		public void setuserID(String userID) {
			this.userID = userID;
		}

		/**
		 * Gets the datetime.
		 * @return datetime. i.e. the number of milliseconds since January 1, 1970, 00:00:00 GMT
		 */
		public String getdatetime() {
			return datetime;
		}

		public void setdatetime(String datetime) {
			this.datetime = datetime;
		}

  }

  
  public static class PhotoLocMapper 
       extends Mapper<Object, Text, PhotoLocKey, Text>{
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
//      StringTokenizer itr = new StringTokenizer(value.toString());
      
//      String userID = itr.nextToken();
//      String date = itr.nextToken();
//      String time = itr.nextToken();
//      String location = itr.nextToken();
//      
//      StringTokenizer itr2 = new StringTokenizer(location,"/");
//      
//      String country = "";
//      
//      country = itr2.nextToken();
      
      String[] tokens = value.toString().split(" ");
      
      String userID = tokens[0].trim();
      String date = tokens[1].trim();
      String time = tokens[2].trim();
      String location = tokens[3].trim();
      
      System.out.println(userID + date + time + location);
      
      String[] tokens1 = location.toString().split("/");
      String country = tokens1[1].trim();
      System.out.println("country:" + country);
      
      PhotoLocKey newkey = new PhotoLocKey(userID, new String(date+time));

      context.write(newkey, new Text(new String (date + '|' + time + '|' + country)));
    }
  }
  
  public static class PhotoLocReducer 
       extends Reducer<PhotoLocKey,Text,Text,Text> {
	  public static class visitloc implements Comparable<visitloc> {
		  String location;
		  double duration;
		  /**
		 * @return the location
		 */
		public String getLocation() {
			return location;
		}
		/**
		 * @return the duration
		 */
		public double getDuration() {
			return duration;
		}
		
		  public visitloc(String location, double duration) {
			super();
			this.location = location;
			this.duration = duration;
		}
		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof visitloc))
				return false;
			visitloc other = (visitloc) obj;
			if (location == null) {
				if (other.location != null)
					return false;
			} else if (!location.equals(other.location))
				return false;
			return true;
		}
		@Override
		public int compareTo(visitloc v) {
			return this.location.compareToIgnoreCase(v.location);
		}
	  }
    private String previousLocation = "";
    private String previousDate= "";
    private String previousTime = "";
    private String currentLocation = "";
    private String currentDate= "";
    private String currentTime = "";
    private String dateTime = "";
    private String userID= "";
    private double visittime = 0;
    long diff,diffDays,diffHours   = 0 ;
    
    List<visitloc> visitlist = new ArrayList<visitloc>();
    
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");
	Date d1 = null;
	Date d2 = null;
       
    public void reduce(PhotoLocKey key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	System.out.println("in reducer");
    	System.out.println("key:" + key );
    	userID = key.getuserID();
    	boolean locationChange = false;
    	
    	for (Text val : values) {
    		System.out.println("\n" + "-----------");
    		System.out.println(val.toString());
    		
    		StringTokenizer itr = new StringTokenizer(val.toString(),"|");
			currentDate = itr.nextToken();
			currentTime = itr.nextToken();
		    currentLocation = itr.nextToken();
		    
		    System.out.println("currentLocation:" + currentLocation );
		    System.out.println("currentDate:" + currentDate );
		    System.out.println("previousLocation:" + previousLocation );
		    System.out.println("previousDate:" + previousDate );
		    
    		if (previousLocation == "" & previousDate == "" ) {
    			previousLocation = currentLocation; 
    			previousDate = currentDate;
    			previousTime = currentTime;
    			
    		} else if (currentLocation.equals(previousLocation)) {
    			
//    			System.out.println("get date diff " + previousDate + currentDate);
    			
    		} else if ((!currentLocation.equals(previousLocation)) ) {
    			
    			System.out.println("get date diff " + previousDate +"/t"+ currentDate);
    			
    			try {
    				dateTime = previousDate + previousTime;
					d1 = format.parse(dateTime );
					dateTime = currentDate + currentTime;
					d2 = format.parse(dateTime );
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    			
    			
    			diff = d2.getTime() - d1.getTime();
    			diffHours = diff / (60 * 60 * 1000) % 24;
    			diffDays = diff / (24 * 60 * 60 * 1000);
    			
    			visittime = (diffDays + (diffHours / 24));
    			visitlist.add(new visitloc(previousLocation,visittime));
    			System.out.println("visittime" + visittime);
    			previousLocation = currentLocation; 
    			previousDate = currentDate;
    			previousTime = currentTime;
    		} else {
    			System.out.println("in else");
    			System.out.println("get date diff " + previousDate +"/t"+ currentDate);
    			try {
    				dateTime = previousDate + previousTime;
					d1 = format.parse(dateTime );
					dateTime = currentDate + currentTime;
					d2 = format.parse(dateTime );
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    			
    			
    			diff = d2.getTime() - d1.getTime();
    			diffHours = diff / (60 * 60 * 1000) % 24;
    			diffDays = diff / (24 * 60 * 60 * 1000);
    			
    			visittime = (diffDays + (diffHours / 24));
    			visitlist.add(new visitloc(previousLocation,visittime));
    			System.out.println("visittime" + visittime);
    			
    			previousLocation = currentLocation; 
    			previousDate = currentDate;
    			previousTime = currentTime;
    		}
          
        }
        
//    	check for the last visit
    	if (currentLocation.equals(previousLocation)) {
    		System.out.println("get date diff " + previousDate +"/t"+ currentDate);
    		try {
    			dateTime = previousDate + previousTime;
				d1 = format.parse(dateTime );
				dateTime = currentDate + currentTime;
				d2 = format.parse(dateTime );
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			diff = d2.getTime() - d1.getTime();
			diffHours = diff / (60 * 60 * 1000) % 24;
			diffDays = diff / (24 * 60 * 60 * 1000);
			
			visittime =  (diffDays + (diffHours / 24));
			visitlist.add(new visitloc(previousLocation,visittime));
			System.out.println("visittime" + visittime);
    	}
      System.out.println("after for loop");
        
      Collections.sort(visitlist);
      
      String ws_loc = "";
      double maximum = Double.MIN_VALUE;
      double minimum = Double.MAX_VALUE;
      double average = Double.MIN_VALUE;
      double totalTime = 0;
      int noOfVisits = 0;
      String tempLoc = "";
      double tempTime;
      System.out.println(visitlist);
      for(visitloc v: visitlist) {
    	  System.out.println("in for loop");
    	  System.out.println(v);
    	  System.out.println(ws_loc);
    	  tempLoc = v.getLocation();
    	  System.out.println(tempLoc );
    	  tempTime = v.getDuration();
    	  if (ws_loc.equals(tempLoc)) {
    		  noOfVisits = noOfVisits +1;
    		  totalTime = totalTime + tempTime ;
    		  if (maximum < tempTime) {
    			  maximum = tempTime;
    		  }
    		  if (minimum > tempTime) {
    			  minimum = tempTime;
    		  }
    		  
    	  } else if (!ws_loc.equals("") & !(ws_loc.equals(tempLoc)))  {
    		  
    		 average = totalTime / noOfVisits;  

    		 System.out.println(ws_loc + "   " +noOfVisits + "   " + maximum + "   "+ minimum +"   "+average + "   "+ totalTime );
    		 context.write(new Text(key.getuserID()), new Text(new String(ws_loc+ "   " +noOfVisits + "   " + maximum + "   "+ minimum +"   "+average + "   "+ totalTime )));
    		 ws_loc = tempLoc;
    		 totalTime = tempTime;
    		 noOfVisits = 1;
    		 maximum = tempTime;
    		 minimum = tempTime;
    		 average = 0;
    	  } else {
    		  ws_loc = tempLoc;
    		  totalTime = tempTime;
    		  noOfVisits = 1;
    		  maximum = tempTime;
    		  minimum = tempTime;
    		  average = 0;
    	  }
    	  
      }
      
 
      average = totalTime / noOfVisits;  

      System.out.println(ws_loc + "   " +noOfVisits + "   " + maximum + "   "+ minimum +"   "+average + "   "+ totalTime );
      context.write(new Text(key.getuserID()), new Text(new String(tempLoc+ "   " +noOfVisits + "   " + maximum + "   "+ minimum +"   "+average + "   "+ totalTime )));

//      context.write(new Text(key.getuserID()), new Text());
      System.out.println("completed one key in reducer");
    }
  }
  
  public static class NaturalKeyPartitioner extends Partitioner<PhotoLocKey, Text> {

		@Override
		public int getPartition(PhotoLocKey key, Text val, int numPartitions) {
			int hash = key.getuserID().hashCode();
			int partition = hash % numPartitions;
			return partition;
		}

	}

  public static class NaturalKeyGroupingComparator extends WritableComparator {

		/**
		 * Constructor.
		 */
		protected NaturalKeyGroupingComparator() {
			super(PhotoLocKey.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			PhotoLocKey k1 = (PhotoLocKey)w1;
			PhotoLocKey k2 = (PhotoLocKey)w2;
			
			return k1.getuserID().compareTo(k2.getuserID());
		}
	}
  
  public static class CompositeKeyComparator extends WritableComparator {

		/**
		 * Constructor.
		 */
		protected CompositeKeyComparator() {
			super(PhotoLocKey.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			PhotoLocKey k1 = (PhotoLocKey)w1;
			PhotoLocKey k2 = (PhotoLocKey)w2;
			
			int result = k1.getuserID().compareTo(k2.getuserID());
			if(0 == result) {
				result = k1.getdatetime().compareTo(k2.getdatetime());
			}
			return result;
		}
	}
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    @SuppressWarnings("deprecation")
	Job job = new Job(conf);
    
    job.setJobName("photo loc application");
    job.setJarByClass(Assign1PhotoLoc.class);
    
	job.setPartitionerClass(NaturalKeyPartitioner.class);
	job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
	job.setSortComparatorClass(CompositeKeyComparator.class);
 
	job.setMapOutputKeyClass(PhotoLocKey.class);
	job.setMapOutputValueClass(Text.class);
	
    job.setMapperClass(PhotoLocMapper.class);
//    job.setCombinerClass(PhotoLocReducer.class);
    job.setReducerClass(PhotoLocReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
 
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path("output31"));
//    System.exit(job.waitForCompletion(true) ? 0 : 1);
    System.out.println("program started");
//    System.out.println(job.getJobState());
	job.waitForCompletion(true);
//	System.out.println(job.getJobState());
  }
}
