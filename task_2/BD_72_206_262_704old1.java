import java.io.IOException;
import java.util.StringTokenizer;
import java.io.DataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

public class BD_72_206_262_704{
	public static class BatBowlWritable implements WritableComparable<BatBowlWritable>{
		private Text batsman;
		private Text bowler;

		public BatBowlWritable(){
			this.batsman = new Text();
			this.bowler = new Text();
		}

		public BatBowlWritable(Text batsman,Text bowler){
			this.batsman = batsman;
			this.bowler = bowler;
		}

		public void set(Text batsman, Text bowler){
			this.batsman = batsman;
			this.bowler = bowler;
		}
		public Text getBatsman() {
			return batsman;
		    }

		    public Text getBowler() {
			return bowler;
		    }

		    public void setBatsman(Text batsman) {
			       this.batsman = batsman;
		    }

		    public void setBowler(Text bowler) {
			this.bowler = bowler;
		    }

		    public void readFields(DataInput in) throws IOException {
			batsman.readFields(in);
			bowler.readFields(in);
		    }

		    public void write(DataOutput out) throws IOException {
			batsman.write(out);
			bowler.write(out);

		    }

		@Override
		    public String toString() {
			return this.batsman.toString() + "," + this.bowler.toString();
		}
		@Override
		public int compareTo(BatBowlWritable b){
			int batcheck = batsman.compareTo(b.getBatsman());
			
			if (batcheck != 0){
				return batcheck;
			}else{
				int bowlcheck = bowler.compareTo(b.getBowler());
				if (bowlcheck!=0){
					return bowlcheck;
				}else{
					return 0;
				}
			}
		}

	}
	// public static class FullKeyComparator extends WritableComparator {
 
	// 	public FullKeyComparator() {
	// 	    super(BatBowlWritable.class, true);
	// 	}
		
	// 	@SuppressWarnings("rawtypes")
	// 	@Override
	// 	public int compare(WritableComparable wc1, WritableComparable wc2) {
		    
	// 	    BatBowlWritable key1 = (BatBowlWritable) wc1;
	// 	    BatBowlWritable key2 = (BatBowlWritable) wc2;
		    
	// 	    int batCmp = key1.getBatsman().compareTo(key2.getBatsman());
	// 	    if (batCmp != 0) {
	// 		return batCmp;
	// 	    } else {
	// 		int bowlCmp = key1.getBowler().compareTo(key2.getBowler());
	// 		if (bowlCmp != 0) {
	// 		    return bowlCmp;
	// 		} else {
	// 		    return 0;
	// 		}
	// 	    }
	// 	}
	//     }
	public static class WickBallWritable implements WritableComparable<WickBallWritable>{
		private IntWritable wickets;
		private IntWritable deliveries;

		public WickBallWritable(){
			this.wickets = new IntWritable();
			this.deliveries = new IntWritable();
		}
		public WickBallWritable(WickBallWritable m){
			this.wickets = new IntWritable(m.getWickets().get());
			this.deliveries = new IntWritable(m.getDeliveries().get());
		}
		public WickBallWritable(IntWritable wickets, IntWritable deliveries){
			this.deliveries = deliveries;
			this.wickets = wickets;
		}

		public void set(IntWritable wickets, IntWritable deliveries){
			this.deliveries = deliveries;
			this.wickets = wickets;
		}

		   public IntWritable getWickets() {
			return this.wickets;
		    }

		    public IntWritable getDeliveries() {
			return this.deliveries;
		    }

		    public void setWickets(IntWritable wickets) {
			this.wickets = wickets;
		    }

		    public void setDeliveries(IntWritable deliveries) {
			this.deliveries = deliveries;
		    }

		    public void readFields(DataInput in) throws IOException {
			this.wickets.readFields(in);
			this.deliveries.readFields(in);
		    }

		    public void write(DataOutput out) throws IOException {
			this.wickets.write(out);
			this.deliveries.write(out);
		    }
		@Override
		public int compareTo(WickBallWritable w){
			return (this.wickets.compareTo(w.getWickets()));
		}
		@Override
		    public String toString() {
			return this.wickets.toString() + "," + this.deliveries.toString();
		}
	}
	public static class DeliveryWritable implements WritableComparable<DeliveryWritable>{
		private Text batsman;
		private Text bowler;
		private IntWritable wickets;
		private IntWritable deliveries;

		public DeliveryWritable(){
			this.batsman = new Text();
			this.bowler = new Text();
			this.wickets = new IntWritable();
			this.deliveries = new IntWritable();
		}

		public DeliveryWritable(Text batsman,Text bowler,IntWritable wickets, IntWritable deliveries){
			this.batsman = batsman;
			this.bowler = bowler;
			this.deliveries = deliveries;
			this.wickets = wickets;
		}

		public void set(Text batsman, Text bowler,IntWritable wickets, IntWritable deliveries){
			this.batsman = batsman;
			this.bowler = bowler;
			this.deliveries = deliveries;
			this.wickets = wickets;
		}
		public Text getBatsman() {
			return batsman;
		    }

		    public Text getBowler() {
			return bowler;
		    }

		   public IntWritable getWickets() {
			return wickets;
		    }

		    public IntWritable getDeliveries() {
			return deliveries;
		    }

		    public void setBatsman(Text batsman) {
			       this.batsman = batsman;
		    }

		    public void setBowler(Text bowler) {
			this.bowler = bowler;
		    }

		    public void setWickets(IntWritable wickets) {
			this.wickets = wickets;
		    }

		    public void setDeliveries(IntWritable deliveries) {
			this.deliveries = deliveries;
		    }

		    public void readFields(DataInput in) throws IOException {
			batsman.readFields(in);
			bowler.readFields(in);
			wickets.readFields(in);
			deliveries.readFields(in);
		    }

		    public void write(DataOutput out) throws IOException {
			batsman.write(out);
			bowler.write(out);
			wickets.write(out);
			deliveries.write(out);
		    }

		@Override
		    public String toString() {
			return batsman.toString() + "," + bowler.toString() + "," + wickets.toString() + "," + deliveries.toString();
		}
		@Override
		public int compareTo(DeliveryWritable b){
			if ((batsman.toString().compareTo(b.getBatsman().toString())==0) &&  (bowler.toString().compareTo(b.getBowler().toString())==0)){
				return 0;
			}
			if (wickets.get() == b.getWickets().get()){
				//same wickets, need to check deliveries
				if (deliveries.get() == b.getDeliveries().get()){
					//another tie, go for name and call it a day
					return (batsman.toString().compareTo(b.getBatsman().toString()));
				}
				return deliveries.compareTo(b.getDeliveries());
			}
			return (wickets.compareTo(b.getWickets()))*-1;
		}

        }
        
        public static class CustomOutputFormat extends FileOutputFormat<DeliveryWritable, Text> {
		public CustomOutputFormat(){};
		@Override
		public org.apache.hadoop.mapreduce.RecordWriter<DeliveryWritable, Text> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		   //get the current path
		   Path path = FileOutputFormat.getOutputPath(arg0);
		   //create the full path with the output directory plus our filename
		   Path file = getDefaultWorkFile(arg0,"");
	       //create the file in the file system
	       FileSystem fs = path.getFileSystem(arg0.getConfiguration());
	       FSDataOutputStream fileOut = fs.create(file, arg0);
	  
	       //create our record writer with the new file
	       return new CustomRecordWriter(fileOut);
	    }
	  }
	  
	public static class CustomRecordWriter extends RecordWriter<DeliveryWritable, Text> {
	      private DataOutputStream out;
	  
	      public CustomRecordWriter(DataOutputStream stream) {
		  out = stream;
		  try {
		      //out.writeBytes("results:\r\n");
		  }
		  catch (Exception ex) {
		  }  
	      }
	  
	      @Override
	      public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
		  //close our file
		  out.close();
	      }
	  
	      @Override
	      public void write(DeliveryWritable arg0, Text arg1) throws IOException, InterruptedException {
		  //write out our key
		  out.writeBytes(arg0.getBatsman().toString() + ","+arg0.getBowler().toString()+","+arg0.getWickets().toString()+","+arg0.getDeliveries().toString());
		  //loop through all values associated with our key and write them with commas between
		  out.writeBytes("\r\n");  
	      }
	}



	private static class TokenizerMapper extends Mapper<Object, Text, BatBowlWritable, WickBallWritable>{

	private Text batsman = new Text();
	private Text bowler = new Text();
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable zero = new IntWritable(0);
	private BatBowlWritable batbowl = new BatBowlWritable();
	private WickBallWritable wickball = new WickBallWritable();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
                //StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
		//while (itr.hasMoreTokens()){
			String[] data = value.toString().split(",",0);
			int len = data.length;
                	if (data[0].equals("ball")){
				batsman.set(data[4]);bowler.set(data[6]);
				batbowl.set(batsman,bowler);
				//if (data[8].equals("0")){
					if (len > 9) {
						if(data[9].equals("obstructing the field")||data[9].equals("stumped")||data[9].equals("hit wicket") || data[9].equals("caught") || data[9].equals("bowled") || data[9].equals("lbw") || data[9].equals("caught and bowled")){
							wickball.set(one,one);
						}else{
							wickball.set(zero,one);
						}
					}else{
						wickball.set(zero,one);
					}
					context.write(batbowl,wickball);
				//}
			}
		}

	}//}
	private static class SortingMapper extends Mapper<Object, Text, DeliveryWritable, Text>{
		private Text batsman = new Text();
		private Text bowler = new Text();
		private IntWritable wickets = new IntWritable();
		private IntWritable delivery = new IntWritable();
		private DeliveryWritable del = new DeliveryWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			while (itr.hasMoreTokens()){
				String[] data = itr.nextToken().split(",",0);
				batsman.set(data[0]);
				bowler.set(data[1]);
				wickets.set(Integer.parseInt(data[2]));
				delivery.set(Integer.parseInt(data[3].trim()));
				del.set(batsman,bowler,wickets,delivery);
				context.write(del,new Text(""));
			}
		}}
	public static class DeliveryReducer extends Reducer<BatBowlWritable,WickBallWritable,DeliveryWritable,Text> {
    		private IntWritable wicket_result = new IntWritable();
		private IntWritable delivery_result = new IntWritable();
		//private WickBallWritable wickball = new WickBallWritable();
		private DeliveryWritable d = new DeliveryWritable();
		private Text empty = new Text("");
		private Text result = new Text();
		private MultipleOutputs output;

		@Override
		public void setup(Context context)
		{
			output = new MultipleOutputs(context);
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			output.close();
		}
    		public void reduce(BatBowlWritable b, Iterable<WickBallWritable> w, Context context) throws IOException, InterruptedException {
			int sum_wickets = 0; int sum_balls = 0;
			      
      			for (WickBallWritable val : w) {
        			sum_wickets += val.getWickets().get();
				sum_balls += val.getDeliveries().get();
			}
			
			wicket_result.set(sum_wickets);
			delivery_result.set(sum_balls);
			if (sum_balls > 5){
				d.set(b.getBatsman(),b.getBowler(),wicket_result,delivery_result);
				output.write("inter", d,new Text(""));
			}
    		}
	}
	public static void main(String[] args) throws Exception {
 		Configuration conf = new Configuration();
    		Job job = Job.getInstance(conf, "BD_72_206_262_704");
    		job.setJarByClass(BD_72_206_262_704.class);
    		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(DeliveryReducer.class);
		//job.setSortComparatorClass(FullKeyComparator.class);
		job.setReducerClass(DeliveryReducer.class);
    		job.setOutputKeyClass(BatBowlWritable.class);
		job.setOutputValueClass(WickBallWritable.class);
		MultipleOutputs.addNamedOutput(job, "inter", TextOutputFormat.class,DeliveryWritable.class,Text.class);
    		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("/temp"));

		int x = job.waitForCompletion(true) ? 0 : 1;
		
		Job finish = Job.getInstance(conf, "BD_72_206_262_704");
    		finish.setJarByClass(BD_72_206_262_704.class);
    		finish.setMapperClass(SortingMapper.class);
    		finish.setOutputKeyClass(DeliveryWritable.class);
                finish.setOutputValueClass(Text.class);
                finish.setOutputFormatClass(CustomOutputFormat.class);
		//MultipleOutputs.addNamedOutput(finish, "final", TextOutputFormat.class,DeliveryWritable.class,Text.class);
    		FileInputFormat.addInputPath(finish, new Path("/temp"));
		FileOutputFormat.setOutputPath(finish, new Path(args[1]));

    		System.exit(finish.waitForCompletion(true) ? 0 : 1);
  }
}
