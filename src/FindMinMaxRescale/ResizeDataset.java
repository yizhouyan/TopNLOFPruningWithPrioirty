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


public class ResizeDataset {
	
	public static class MapRescale extends Mapper<LongWritable, Text, NullWritable, Text> {
		private double originalTotalNumber = 80000;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitdata = value.toString().split(",");
			long original_id = Long.valueOf(splitdata[0]);
			Double first_data = Double.valueOf(splitdata[1]);
			Double second_data = Double.valueOf(splitdata[2]);
			// output 9 times
			if(first_data < originalTotalNumber && second_data < originalTotalNumber)
				context.write(NullWritable.get(),new Text(original_id+ "," + first_data + "," +second_data));
			
		} // end map function

	} // end Map Class

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		Job job = Job.getInstance(conf, "Resize dataset");
		
		job.setJarByClass(ResizeDataset.class);
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
		ResizeDataset rescale = new ResizeDataset();
		rescale.run(args);
	}
}
