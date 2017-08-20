package lof.pruning.secondknn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricObjectMore;
import metricspace.MetricSpaceUtility;
import metricspace.Record;
import metricspace.coreInfoKNNs;
import sampling.CellStore;
import util.SQConfig;

public class CalKdistanceSecond {
	/**
	 * default Map class.
	 *
	 * @author Yizhou Yan
	 * @version Jan 10, 2017
	 */

	static enum Counters {
		// PhaseTime, OutputTime, CannotFindKPointsAsNeighbors
		CountDuplicatePoints,
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "TopN-LOF: Update KNNs");

		job.setJarByClass(CalKdistanceSecond.class);
		job.setMapperClass(CalKdistSecondMapper.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(CalKdistSecondReducer.class);
		 job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
//		job.setNumReduceTasks(0);

		String strFSName = conf.get("fs.default.name");
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.strKdistanceOutput)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strKdistFinalOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strKdistFinalOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strKnnPartitionPlan)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strKnnCellsOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strDimCorrelationOutput)));
//		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strTopNFirstSummary)));

		/** print job parameter */
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, 10));
		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) throws Exception {
		CalKdistanceSecond findKnnAndSupporting = new CalKdistanceSecond();
		findKnnAndSupporting.run(args);
	}
}
