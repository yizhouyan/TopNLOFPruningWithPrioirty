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


public class RescaleDataset {
	
	public static class MapRescale extends Mapper<LongWritable, Text, NullWritable, Text> {
		private double minValue_1;
		private double maxValue_1;
		private double minValue_2;
		private double maxValue_2;
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			Configuration conf = context.getConfiguration();
			try {
				URI[] cacheFiles = new URI[0];
				cacheFiles = context.getCacheFiles();
				if (cacheFiles == null || cacheFiles.length != 1)
					return;
				for (URI path : cacheFiles) {
					String filename = path.toString();
					FileSystem fs = FileSystem.get(conf);
					FileStatus[] stats = fs.listStatus(new Path(filename));
					for (int i = 0; i < stats.length; ++i) {
						if (!stats[i].isDirectory()) {
							System.out.println("Reading partition plan from " + stats[i].getPath().toString());
							FSDataInputStream currentStream;
							BufferedReader currentReader;
							currentStream = fs.open(stats[i].getPath());
							currentReader = new BufferedReader(new InputStreamReader(currentStream));
							String line;
							while ((line = currentReader.readLine()) != null) {
								/** parse line */

								String[] splitsStr = line.split(SQConfig.sepStrForKeyValue)[1]
										.split(SQConfig.sepStrForRecord);
								minValue_1 = Double.parseDouble(splitsStr[0]);
								maxValue_1 = Double.parseDouble(splitsStr[1]);
								minValue_2 = Double.parseDouble(splitsStr[2]);
								maxValue_2 = Double.parseDouble(splitsStr[3]);
							}
							currentReader.close();
							currentStream.close();
						}
					}
				}
			} catch (IOException ioe) {
				System.err
						.println("Caught exception while getting cached files");
			}
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitdata = value.toString().split(",");
//			FileSplit fileSplit = (FileSplit)context.getInputSplit();
//			String filename = fileSplit.getPath().getName();
//			System.out.println(filename);
			Double first_data = Double.valueOf(splitdata[1]);
			Double second_data = Double.valueOf(splitdata[2]);
			first_data = (first_data -(minValue_1-0.01)) / (maxValue_1-minValue_1+0.015) * 500000;
			second_data = (second_data - (minValue_2-0.01)) / (maxValue_2-minValue_2+0.015) * 500000;
			context.write(NullWritable.get(),new Text(splitdata[0] + "," + first_data + "," + second_data));
		} // end map function

	} // end Map Class

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		Job job = Job.getInstance(conf, "Rescale dataset");
		String strFSName = conf.get("fs.default.name");
		job.addCacheFile(new URI(strFSName + "/lof/datasets/minmax"));
		job.setJarByClass(RescaleDataset.class);
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
		RescaleDataset rescale = new RescaleDataset();
		rescale.run(args);
	}
}
