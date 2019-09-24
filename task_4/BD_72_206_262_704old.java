import java.io.IOException;
import java.util.StringTokenizer;

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
import java.util.HashMap;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BD_72_206_262_704{
	public static class VenueBatWritable implements WritableComparable<VenueBatWritable>{
		private Text venue;
		private Text batsman;

		public VenueBatWritable(){
			this.venue = new Text();
			this.batsman = new Text();
		}

		public VenueBatWritable(Text venue,Text batsman){
			this.batsman = batsman;
			this.venue = venue;
		}

		public void set(Text venue, Text batsman){
			this.batsman = batsman;
			this.venue = venue;
		}
		public Text getBatsman() {
			return batsman;
		    }

		    public Text getVenue() {
			return venue;
		    }

		    public void setBatsman(Text batsman) {
			       this.batsman = batsman;
		    }

		    public void setVenue(Text venue) {
			this.venue = venue;
		    }

		    public void readFields(DataInput in) throws IOException {
			venue.readFields(in);
			batsman.readFields(in);
		    }

		    public void write(DataOutput out) throws IOException {
			venue.write(out);
			batsman.write(out);

		    }

		@Override
		    public String toString() {
			return this.venue.toString() + "," + this.batsman.toString();
		}
		@Override
		public int compareTo(VenueBatWritable b){
			int batcheck = batsman.compareTo(b.getBatsman());
			
			if (batcheck != 0){
				return batcheck;
			}else{
				int venuecheck = venue.compareTo(b.getVenue());
				if (venuecheck!=0){
					return venuecheck;
				}else{
					return 0;
				}
			}
		}

	}
	
	public static class RunDeliveryWritable implements WritableComparable<RunDeliveryWritable>{
		private IntWritable runs;
		private IntWritable deliveries;

		public RunDeliveryWritable(){
			this.runs = new IntWritable();
			this.deliveries = new IntWritable();
		}
		public RunDeliveryWritable(RunDeliveryWritable m){
			this.runs = new IntWritable(m.getRuns().get());
			this.deliveries = new IntWritable(m.getDeliveries().get());
		}
		public RunDeliveryWritable(IntWritable runs, IntWritable deliveries){
			this.deliveries = deliveries;
			this.runs = runs;
		}

		public void set(IntWritable runs, IntWritable deliveries){
			this.deliveries = deliveries;
			this.runs = runs;
		}

		   public IntWritable getRuns() {
			return this.runs;
		    }

		    public IntWritable getDeliveries() {
			return this.deliveries;
		    }

		    public void setRuns(IntWritable runs) {
			this.runs = runs;
		    }

		    public void setDeliveries(IntWritable deliveries) {
			this.deliveries = deliveries;
		    }

		    public void readFields(DataInput in) throws IOException {
			this.runs.readFields(in);
			this.deliveries.readFields(in);
		    }

		    public void write(DataOutput out) throws IOException {
			this.runs.write(out);
			this.deliveries.write(out);
                    }
                    
		@Override
		public int compareTo(RunDeliveryWritable w){
			return (this.runs.compareTo(w.getRuns()));
		}
		@Override
		    public String toString() {
			return runs.toString() + "," + deliveries.toString();
		}
	}
	public static class DeliveryWritable implements WritableComparable<DeliveryWritable>{
		private Text batsman;
		private Text venue;
		private IntWritable runs;
		private IntWritable deliveries;

		public DeliveryWritable(){
			this.batsman = new Text();
			this.venue = new Text();
			this.runs = new IntWritable();
			this.deliveries = new IntWritable();
		}

		public DeliveryWritable(Text venue,Text batsman,IntWritable runs, IntWritable deliveries){
			this.batsman = batsman;
			this.venue = venue;
			this.deliveries = deliveries;
			this.runs = runs;
		}

		public void set(Text venue, Text batsman,IntWritable runs, IntWritable deliveries){
			this.batsman = batsman;
			this.venue = venue;
			this.deliveries = deliveries;
			this.runs = runs;
		}
		public Text getBatsman() {
			return batsman;
		    }

		    public Text getVenue() {
			return this.venue;
		    }

		   public IntWritable getRuns() {
			return runs;
		    }

		    public IntWritable getDeliveries() {
			return deliveries;
		    }

		    public void setBatsman(Text batsman) {
			       this.batsman = batsman;
		    }

		    public void setVenue(Text venue) {
			this.venue = venue;
		    }

		    public void setWickets(IntWritable runs) {
			this.runs = runs;
		    }

		    public void setDeliveries(IntWritable deliveries) {
			this.deliveries = deliveries;
		    }

		    public void readFields(DataInput in) throws IOException {
			venue.readFields(in);
			batsman.readFields(in);
			runs.readFields(in);
			deliveries.readFields(in);
		    }

		    public void write(DataOutput out) throws IOException {
			venue.write(out);
			batsman.write(out);
			runs.write(out);
			deliveries.write(out);
		    }

		@Override
		    public String toString() {
			return venue.toString() + "," + batsman.toString();//+runs.toString() + "," + deliveries.toString();
		}
		@Override
		public int compareTo(DeliveryWritable b){
			if ((batsman.compareTo(b.getBatsman())==0) &&  (venue.compareTo(b.getVenue())==0)){
				return 0;
                        }
                        float strikerate1 = runs.get()*100/deliveries.get();
                        float strikerate2 = b.getRuns().get()*100/b.getDeliveries().get();

			if (strikerate1 == strikerate2){
				//same rate, need to check deliveries
				// if (runs.get() == b.getRuns().get()){
				// 	//another tie, go for name and call it a day
				// 	return (batsman.toString().compareTo(b.getBatsman().toString()));
				// }
				return runs.compareTo(b.getRuns())*-1;
			}
                        return ((int)strikerate1-(int)strikerate2);
		}

	}
	private static class TokenizerMapper extends Mapper<Object, Text, VenueBatWritable, RunDeliveryWritable>{

                private Text venue = new Text();
                private Text batsman = new Text();
                private IntWritable runs = new IntWritable();
                private final static IntWritable one = new IntWritable(1);
                //private final static IntWritable zero = new IntWritable(0);
                private VenueBatWritable vb = new VenueBatWritable();
                private RunDeliveryWritable rd = new RunDeliveryWritable();
                private Text current_venue = new Text();
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
                        //StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
                        
                       // while (itr.hasMoreTokens()){
                                //String[] data = itr.nextToken().split(",",0);
                                //int len = data.length;
                                String[] data = value.toString().split(",",0);
                                if (data[0].equals("ball")){
                                        if(data[8].equals("0")){ //not an extra
                                                batsman.set(data[4]);venue.set(current_venue.toString());
                                                runs.set(Integer.parseInt(data[7]));
                                                vb.set(venue,batsman);
                                                rd.set(runs,one);
                                                context.write(vb,rd);
                                        }
                                        
                                }else{
                                        //info
                                        if (data[0].equals("info") && data[1].equals("venue")){
                                                current_venue.set(data[2]);
                                        }
                                }
                        //}

                }}
	private static class SortingMapper extends Mapper<Object, Text, Text, Text>{
		private Text venue = new Text();
		private Text batsman = new Text();
		private IntWritable runs = new IntWritable();
		private IntWritable deliveries = new IntWritable();
                private VenueBatWritable vb = new VenueBatWritable();
                private HashMap hs = new HashMap();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			//StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			//while (itr.hasMoreTokens()){
                                //String[] data = itr.nextToken().split(",",0);
                                String[] data = value.toString().split(",",0);
                                if (!hs.containsKey(data[0].trim())){
                                        venue.set(data[0].trim());
                                        batsman.set(data[1].trim());
                                        vb.set(venue,batsman);
                                        hs.put(venue.toString(),1);
				        context.write(new Text(vb.toString()),new Text(""));
                                }
				
			//}
		}}
	public static class DeliveryReducer extends Reducer<VenueBatWritable,RunDeliveryWritable,DeliveryWritable,Text> {
    		private IntWritable runs_result = new IntWritable();
		private IntWritable delivery_result = new IntWritable();
		
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
    		public void reduce(VenueBatWritable b, Iterable<RunDeliveryWritable> w, Context context) throws IOException, InterruptedException {
			int sum_runs = 0; int sum_balls = 0;
			      
      			for (RunDeliveryWritable val : w) {
        			sum_runs += val.getRuns().get();
				sum_balls += val.getDeliveries().get();
			}
			
			runs_result.set(sum_runs);
			delivery_result.set(sum_balls);
			if (sum_balls > 10){
				d.set(b.getVenue(),b.getBatsman(),runs_result,delivery_result);
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
		//job.setGroupingComparatorClass(FullKeyComparator.class);
		job.setReducerClass(DeliveryReducer.class);
    		job.setOutputKeyClass(VenueBatWritable.class);
		job.setOutputValueClass(RunDeliveryWritable.class);
		MultipleOutputs.addNamedOutput(job, "inter", TextOutputFormat.class,DeliveryWritable.class,Text.class);
    		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("/tempo"));
                int x = job.waitForCompletion(true) ? 0 : 1;
		
		Job finish = Job.getInstance(conf, "BD_72_206_262_704");
    		finish.setJarByClass(BD_72_206_262_704.class);
    		finish.setMapperClass(SortingMapper.class);
    		finish.setOutputKeyClass(Text.class);
		finish.setOutputValueClass(Text.class);
		//MultipleOutputs.addNamedOutput(finish, "final", TextOutputFormat.class,DeliveryWritable.class,Text.class);
    		FileInputFormat.addInputPath(finish, new Path("/tempo"));
		FileOutputFormat.setOutputPath(finish, new Path(args[1]));

    		System.exit(finish.waitForCompletion(true) ? 0 : 1);
  }
}

