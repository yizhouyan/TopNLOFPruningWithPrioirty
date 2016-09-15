package FindMinMaxRescale;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.SQConfig;


public class ScalePlanetDataset {
	
	public static class MapRescale extends Mapper<LongWritable, Text, NullWritable, Text> {
		private long originalTotalNumber = 3053883296l;
		private double shiftUnit_1 = 360;
		private double shiftUnit_2 = 180; 
		public String generateText(int i, int j, long original_id, double first_data, double second_data){
			long output_id = original_id + originalTotalNumber * (i+2*j);
			double output_first_data = first_data + shiftUnit_1 * i;
			double output_second_data = second_data + shiftUnit_2 * j;
			String generatedText = output_id +"," + output_first_data + "," + output_second_data;
			return generatedText;
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitdata = value.toString().split(",");
			long original_id = Long.valueOf(splitdata[0]);
			Double first_data = Double.valueOf(splitdata[1]);
			Double second_data = Double.valueOf(splitdata[2]);
			// output 9 times
			for(int i = 0; i < 2; i++)
				for(int j = 0; j < 2; j++){
					context.write(NullWritable.get(),new Text(generateText(i,j,original_id, first_data, second_data)));
				}
			
		} // end map function

	} // end Map Class

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		Job job = Job.getInstance(conf, "Scale Planet dataset to Terabytes");
		
		job.setJarByClass(ScalePlanetDataset.class);
		job.setMapperClass(MapRescale.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ScalePlanetDataset rescale = new ScalePlanetDataset();
		rescale.run(args);
	}
}
