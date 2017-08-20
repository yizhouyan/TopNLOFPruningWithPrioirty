package lof.pruning.firstknn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import lof.pruning.firstknn.CalKdistanceFirstMultiDim.Counters;
import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricKey;
import metricspace.MetricObject;
import metricspace.MetricSpaceUtility;
import metricspace.PriorityComparator;
import metricspace.PriorityGroupComparator;
//import metricspace.PriorityPartitioner;
import metricspace.Record;
import sampling.CellStore;
import util.PriorityQueue;
import util.SQConfig;

public class CalKdistanceFirstMultiDim {
	/**
	 * default Map class.
	 *
	 * @author Yizhou Yan
	 * @version Dec 25, 2016
	 */

	/** number of object pairs to be computed */
	public static enum Counters {
		CellPrunedPoints, LRDPrunedPoints, FinalPrunedPoints, FinalOutputPoints
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "Cell-Based KNN Searching: With Simple Priority");

		job.setJarByClass(CalKdistanceFirstMultiDim.class);
		job.setMapperClass(FirstKNNFinderMapper.class);

		/** set multiple output path */
		MultipleOutputs.addNamedOutput(job, "partitionplan", TextOutputFormat.class, LongWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "kdistance", TextOutputFormat.class, LongWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "topnLOF", TextOutputFormat.class, LongWritable.class, Text.class);

		job.setReducerClass(FirstKNNFinderReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		// job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		 job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
//		job.setNumReduceTasks(0);
		// job.setPartitionerClass(PriorityPartitioner.class);
		// job.setSortComparatorClass(PriorityComparator.class);
		// job.setGroupingComparatorClass(PriorityGroupComparator.class);

		String strFSName = conf.get("fs.default.name");
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.dataset)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strKnnSummaryOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strKnnSummaryOutput)));
		/** set multiple output path */
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strPartitionPlanOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strCellsOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strDimCorrelationOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strPartitionAjacencyOutput)));
		/** print job parameter */
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, 10));
		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) {
		CalKdistanceFirstMultiDim findKnnAndSupporting = new CalKdistanceFirstMultiDim();
		try {
			findKnnAndSupporting.run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
