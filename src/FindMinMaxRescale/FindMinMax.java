package FindMinMaxRescale;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FindMinMax {
	public static class MapFindMinMax extends Mapper<LongWritable, Text, Text, Text> {
		double min_1 = Double.POSITIVE_INFINITY;
		double min_2 = Double.POSITIVE_INFINITY;
		double max_1 = Double.NEGATIVE_INFINITY;
		double max_2 = Double.NEGATIVE_INFINITY;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] splitdata = value.toString().split(",");
			Double first_data = Double.valueOf(splitdata[1]);
			Double second_data = Double.valueOf(splitdata[2]);

			if (first_data > max_1)
				max_1 = first_data;

			if (first_data < min_1)
				min_1 = first_data;

			if (second_data > max_2)
				max_2 = second_data;

			if (second_data < min_2)
				min_2 = second_data;

		} // end map function

		public void cleanup(Context context) throws IOException, InterruptedException {
			Text dat = new Text(min_1 + "," + max_1 + "," + min_2 + "," + max_2);
			context.write(new Text(""), new Text(dat));
		} // end map function

	} // end Map Class

	public static class ReducerFindMinMax extends Reducer<Text, Text, Text, Text> {
		double min_1 = Double.POSITIVE_INFINITY;
		double min_2 = Double.POSITIVE_INFINITY;
		double max_1 = Double.NEGATIVE_INFINITY;
		double max_2 = Double.NEGATIVE_INFINITY;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				String line = value.toString();
				String[] linesplits = line.split(",");
				if (Double.valueOf(linesplits[0]) < min_1)
					min_1 = Double.valueOf(linesplits[0]);
				if (Double.valueOf(linesplits[1]) > max_1)
					max_1 = Double.valueOf(linesplits[1]);
				if (Double.valueOf(linesplits[2]) < min_2)
					min_2 = Double.valueOf(linesplits[2]);
				if (Double.valueOf(linesplits[3]) > max_2)
					max_2 = Double.valueOf(linesplits[3]);
			}
			Text dat = new Text(min_1 + "," + max_1 + "," + min_2 + "," + max_2);
			context.write(new Text(""), new Text(dat));
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Rescale dataset");

		job.setJarByClass(FindMinMax.class);
		job.setMapperClass(MapFindMinMax.class);
		job.setReducerClass(ReducerFindMinMax.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

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
		FindMinMax rescale = new FindMinMax();
		rescale.run(args);
	}
}
