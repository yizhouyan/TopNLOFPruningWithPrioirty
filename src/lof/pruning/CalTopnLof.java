package lof.pruning;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import metricspace.MetricObject;
import metricspace.Record;
import util.SQConfig;

public class CalTopnLof {

	public static void main(String[] args) throws Exception {
		CalTopnLof callof = new CalTopnLof();
		callof.run(args);
	}

	public static class CalLOFMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

		private PriorityQueue topnLOF = new PriorityQueue(PriorityQueue.SORT_ORDER_ASCENDING);
		private int topNNumber = 100;

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topNNumber = conf.getInt(SQConfig.strLOFTopNThreshold, 100);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] valuePart = value.toString().split(SQConfig.sepStrForKeyValue);
			long nid = Long.valueOf(valuePart[0]);
			float lofValue = Float.valueOf(valuePart[1]);
			if (topnLOF.size() < topNNumber) {
				topnLOF.insert(nid, lofValue);
			} else if (lofValue > topnLOF.getPriority()) {
				topnLOF.pop();
				topnLOF.insert(nid, lofValue);
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			while (topnLOF.size() > 0) {
				context.write(NullWritable.get(),
						new Text(topnLOF.getValue() + SQConfig.sepStrForRecord + topnLOF.getPriority()));
				topnLOF.pop();
			}
		}
	}

	public static class CalLOFReducer extends Reducer<NullWritable, Text, LongWritable, Text> {
		LongWritable outputKey = new LongWritable();
		Text outputValue = new Text();

		private PriorityQueue topnLOF = new PriorityQueue(PriorityQueue.SORT_ORDER_ASCENDING);
		HashSet<Long> hs = new HashSet<Long>();
		private int topNNumber = 100;

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topNNumber = conf.getInt(SQConfig.strLOFTopNThreshold, 100);
		}

		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				String[] valuePart = value.toString().split(SQConfig.sepStrForRecord);
				long nid = Long.valueOf(valuePart[0]);
				float lofValue = Float.valueOf(valuePart[1]);
				if (!hs.contains(nid)) {
					if (topnLOF.size() < topNNumber) {
						hs.add(nid);
						topnLOF.insert(nid, lofValue);
					} else if (lofValue > topnLOF.getPriority()) {
						hs.remove(topnLOF.getValue());
						topnLOF.pop();
						hs.add(nid);
						topnLOF.insert(nid, lofValue);
					}
				}
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			while (topnLOF.size() > 0) {
				outputKey.set(topnLOF.getValue());
				outputValue.set(topnLOF.getPriority() + "");
				context.write(outputKey, outputValue);
				topnLOF.pop();
			}
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "DDLOF: Calculate final top N lof");
		String strFSName = conf.get("fs.default.name");

		job.setJarByClass(CalTopnLof.class);
		job.setMapperClass(CalLOFMapper.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class); ////////////////////
		job.setReducerClass(CalLOFReducer.class);
		job.setNumReduceTasks(1);
		MultipleInputs.addInputPath(job, new Path(conf.get(SQConfig.strLOFOutput)),
                TextInputFormat.class, CalLOFMapper.class);
        MultipleInputs.addInputPath(job, new Path(conf.get(SQConfig.strTopNFirstSummary)),
                TextInputFormat.class, CalLOFMapper.class);
		//FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.strLOFOutput)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strTOPNLOFOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strTOPNLOFOutput)));

		/** print job parameter */
		System.err.println("input path: " + conf.get(SQConfig.strLOFOutput));
		System.err.println("output path: " + conf.get(SQConfig.strTOPNLOFOutput));
		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

}
