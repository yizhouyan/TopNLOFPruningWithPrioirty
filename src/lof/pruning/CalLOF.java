package lof.pruning;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import metricspace.MetricObjectLOF;
import metricspace.Record;
import metricspace.coreInfoKNNs;
import sampling.CellStore;
import util.SQConfig;

public class CalLOF {
	private static int dim;

	public static void main(String[] args) throws Exception {
		CalLOF callof = new CalLOF();
		callof.run(args);
	}

	public static class CalLOFMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		/** value of K */
		int K;

		private IntWritable interKey = new IntWritable();
		private Text interValue = new Text();

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			dim = conf.getInt(SQConfig.strDimExpression, -1);
			K = conf.getInt(SQConfig.strK, 1);
		}

		/**
		 * used to calculate LOF same partition as the first round input format:
		 * key:nid value: partition id, original type, lrd, lof, whosesupport
		 * (KNN's nid and lrd) output format: (Core area)key: partition id ||
		 * value: nid, type(S or C), LRD, whoseSupport, (KNN's nid and dist)
		 * (Support area)key: partition id || value: nid, type(S or C), LRD
		 * 
		 * @author yizhouyan
		 */

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] valuePart = value.toString().split(SQConfig.sepStrForKeyValue);
			long nid = Long.valueOf(valuePart[0]);
			String[] strValue = valuePart[1].split(SQConfig.sepStrForRecord);
			int Core_partition_id = Integer.valueOf(strValue[0]);
			char orginalTag = strValue[1].charAt(0);
			float lrdValue = Float.valueOf(strValue[2]);
			float lofValue = Float.valueOf(strValue[3]);
			String whoseSupport = strValue[4];
			int offset = strValue[0].length() + strValue[1].length() + strValue[2].length() + strValue[3].length()
					+ strValue[4].length() + 5;
			String knn_id_dist = valuePart[1].substring(offset, valuePart[1].length());

			// output Core partition node
			interKey.set(Core_partition_id);
			interValue.set(nid + ",C," + orginalTag + "," + lrdValue + "," + lofValue + "," + knn_id_dist);
			context.write(interKey, interValue);

			// output Support partition node
			if (whoseSupport.length() != 0) {
				String[] whosePar = whoseSupport.split(SQConfig.sepSplitForIDDist);
				for (int i = 0; i < whosePar.length; i++) {
					int tempid = Integer.valueOf(whosePar[i]);
					interKey.set(tempid);
					interValue.set(nid + ",S," + lrdValue);
					context.write(interKey, interValue);
				}
			}
		}
	}

	public static class CalLOFReducer extends Reducer<IntWritable, Text, LongWritable, Text> {
		int K;
		LongWritable outputKey = new LongWritable();
		Text outputValue = new Text();

		private static float thresholdLof = 10.0f;
		private PriorityQueue topnLOF = new PriorityQueue(PriorityQueue.SORT_ORDER_ASCENDING);
		private int topNNumber = 100;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			dim = conf.getInt(SQConfig.strDimExpression, -1);
			K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
			thresholdLof = conf.getFloat(SQConfig.strLOFThreshold, 1.0f);
			topNNumber = conf.getInt(SQConfig.strLOFTopNThreshold, 100);
			
			try {
				URI[] cacheFiles = context.getCacheArchives();

				if (cacheFiles == null || cacheFiles.length < 1) {
					System.out.println("not enough cache files");
					return;
				}
				for (URI path : cacheFiles) {
					String filename = path.toString();
					FileSystem fs = FileSystem.get(conf);

					FileStatus[] stats = fs.listStatus(new Path(filename));
					for (int i = 0; i < stats.length; ++i) {
						if (!stats[i].isDirectory()
								&& stats[i].getPath().toString().contains(conf.get(SQConfig.strTopNFirstSummary))
								&& stats[i].getPath().toString().contains("part-r-00000")) {
							// System.out.println("Reading threshold from " +
							// stats[i].getPath().toString());
							FSDataInputStream currentStream;
							BufferedReader currentReader;
							currentStream = fs.open(stats[i].getPath());
							currentReader = new BufferedReader(new InputStreamReader(currentStream));
							String line = currentReader.readLine();
							thresholdLof = Float.parseFloat(line.split(SQConfig.sepStrForKeyValue)[1]);
							System.out.println("The threshold for LOF is " + thresholdLof);
						}
					} // end for (int i = 0; i < stats.length; ++i)
				} // end for (URI path : cacheFiles)

			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files");
			}
		}

		/**
		 * (Support area)key: partition id || value: nid, type(S), lrd
		 * 
		 * @return
		 */
		private MetricObjectLOF parseSupportObject(int key, String strInput) {
			String[] tempSubString = strInput.split(SQConfig.sepStrForRecord);
			Record obj = new Record(Long.valueOf(tempSubString[0]));
			char curTag = tempSubString[1].charAt(0);
			float curlrd = -1;
			curlrd = Float.parseFloat(tempSubString[2]);
			return new MetricObjectLOF(obj, curTag, curlrd);
		}

		/**
		 * (Core area)key: partition id || value: nid, type(C), orgType, lrd,
		 * lof, (KNN's nid and lrd)
		 * 
		 * @param key
		 * @param strInput
		 * @return
		 */
		private MetricObjectLOF parseCoreObject(int key, String strInput, Context context) {
			String[] splitStrInput = strInput.split(SQConfig.sepStrForRecord);
			Record obj = new Record(Long.valueOf(splitStrInput[0]));
			char curTag = splitStrInput[1].charAt(0);
			char orgTag = splitStrInput[2].charAt(0);
			float curLrd = Float.parseFloat(splitStrInput[3]);
			float curLof = Float.parseFloat(splitStrInput[4]);

			Map<Long, Float> knnInDetail = new HashMap<Long, Float>();
			for (int i = 0; i < K; i++) {
				String[] tempSplit = splitStrInput[5 + i].split(SQConfig.sepSplitForIDDist);
				if (tempSplit.length > 1) {
					long knnid = Long.parseLong(tempSplit[0]);
					float knnlrd = Float.parseFloat(tempSplit[1]);
					knnInDetail.put(knnid, knnlrd);
				} else {
					break;
				}
			}
			return new MetricObjectLOF(obj, curTag, orgTag, knnInDetail, curLrd, curLof);
		}

		/**
		 * find knn for each string in the key.pid format of each value in
		 * values
		 * 
		 */
		@SuppressWarnings("unchecked")
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Vector<MetricObjectLOF> coreData = new Vector<MetricObjectLOF>();
			HashMap<Long, Float> hm_lrd = new HashMap<Long,Float>();
			for (Text value : values) {
				if (value.toString().contains("S")) {
					MetricObjectLOF mo = parseSupportObject(key.get(), value.toString());
					hm_lrd.put(((Record) mo.getObj()).getRId(), mo.getLrdValue());
				} else if (value.toString().contains("C")) {
					MetricObjectLOF mo = parseCoreObject(key.get(), value.toString(), context);
					coreData.addElement(mo);
					hm_lrd.put(((Record) mo.getObj()).getRId(), mo.getLrdValue());
				}
			}
			long begin = System.currentTimeMillis();
			for (MetricObjectLOF o_S : coreData) {
				CalLOFForSingleObject(context, o_S, hm_lrd);
			}
			long end = System.currentTimeMillis();
			long second = (end - begin) / 1000;
			System.err.println("computation time " + " takes " + second + " seconds");
		}

		/**
		 * need optimization
		 * 
		 * @throws InterruptedException
		 */
		private void CalLOFForSingleObject(Context context, MetricObjectLOF o_S, HashMap<Long, Float> hm)
				throws IOException, InterruptedException {

			float lof_core = 0.0f;
			boolean canCalLOF = true;
			if (o_S.getOrgType() == 'L') {
				for (Map.Entry<Long, Float> entry : o_S.getKnnInDetail().entrySet()) {
					long keyMap = entry.getKey();
					float valueMap = entry.getValue();
					if (valueMap <= 0) {
						if (hm.containsKey(keyMap))
							valueMap = hm.get(keyMap);
						else if(valueMap < 0) {
//							System.out.println("Cannot find lrd..maybe pruned");
							canCalLOF = false;
							break;
						}
					}
					if (valueMap == 0 || o_S.getLrdValue() == 0)
						lof_core = lof_core;
					else
						lof_core += valueMap / o_S.getLrdValue() * 1.0f;
				}
				lof_core = lof_core / K * 1.0f;
				o_S.setLofValue(lof_core);
			}
			if (Float.isNaN(lof_core) || Float.isInfinite(lof_core))
				lof_core = 0;
			if (canCalLOF && lof_core > thresholdLof) {
				if (topnLOF.size() < topNNumber) {
					topnLOF.insert(((Record) o_S.getObj()).getRId(), lof_core);

				} else if (lof_core > topnLOF.getPriority()) {
					topnLOF.pop();
					topnLOF.insert(((Record) o_S.getObj()).getRId(), lof_core);
					if(thresholdLof < topnLOF.getPriority())
						thresholdLof = topnLOF.getPriority();
				}
			} // end if 
		} // end Function
		public void cleanup(Context context) throws IOException, InterruptedException {
			// output top n LOF value
			while(topnLOF.size()>0){
				context.write(new LongWritable(topnLOF.getValue()), new Text(topnLOF.getPriority()+""));
				topnLOF.pop();
			}
		} // end Function
	} // end Reduce Function

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "Calculate lof");
		String strFSName = conf.get("fs.default.name");

		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strTopNFirstSummary)));
		job.setJarByClass(CalLOF.class);
		job.setMapperClass(CalLOFMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class); ////////////////////
		job.setReducerClass(CalLOFReducer.class);
		// job.setNumReduceTasks(2);
		job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.strLRDOutput)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strLOFOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strLOFOutput)));

		/** print job parameter */
		System.err.println("input path: " + conf.get(SQConfig.strLRDOutput));
		System.err.println("output path: " + conf.get(SQConfig.strLOFOutput));
		System.err.println("value of K: " + conf.get(SQConfig.strK));
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, 10));

		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

}
