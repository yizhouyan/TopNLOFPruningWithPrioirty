package lof.pruning;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
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

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricObject;
import metricspace.MetricSpaceUtility;
import metricspace.Record;
import metricspace.coreInfoKNNs;
import util.SortByDist;
import util.SQConfig;

public class CalLRD {
	private static int dim;

	public static class CalLRDMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		/** value of K */
		int K;
		private IntWritable interKey = new IntWritable();
		private Text interValue = new Text();

		@SuppressWarnings("unchecked")
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			dim = conf.getInt(SQConfig.strDimExpression, -1);
			K = conf.getInt(SQConfig.strK, 1);
		}

		/**
		 * used to calculate LRD same partition as the first round input format:
		 * key: nid || value: partition id, orgType, k-distance, lrd, lof,
		 * whoseSupport,(KNN's nid and dist and kdist and lrd)
		 * 
		 * output format: (Core area)key: partition id || value: nid, type(C),
		 * orgType, k-distance, lrd, lof, whoseSupport, (KNN's nid and dist and
		 * kdist and lrd)
		 * 
		 * (Support area)key: partition id || value: nid, type(S), orgType,
		 * k-distance, lrd, lof
		 * 
		 * @author yizhouyan
		 */

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] valuePart = value.toString().split(SQConfig.sepStrForKeyValue);
			long nid = Long.valueOf(valuePart[0]);
			String[] strValue = valuePart[1].split(SQConfig.sepStrForRecord);
			int Core_partition_id = Integer.valueOf(strValue[0]);
			char orginalTag = strValue[1].charAt(0);
			float kdist = Float.valueOf(strValue[2]);
			float lrdValue = Float.valueOf(strValue[3]);
			float lofValue = Float.valueOf(strValue[4]);
			String whoseSupport = strValue[5];
			int offset = strValue[0].length() + strValue[1].length() + strValue[2].length() + strValue[3].length()
					+ strValue[4].length() + strValue[5].length() + 6;
			String knn_id_dist = valuePart[1].substring(offset, valuePart[1].length());

			// output Core partition node
			interKey.set(Core_partition_id);
			interValue.set(nid + ",C," + orginalTag + "," + kdist + "," + lrdValue + "," + lofValue + "," + whoseSupport
					+ "," + knn_id_dist);
			context.write(interKey, interValue);

			// output Support partition node
			if (whoseSupport.length() != 0) {
				String[] whosePar = whoseSupport.split(SQConfig.sepSplitForIDDist);
				for (int i = 0; i < whosePar.length; i++) {
					int tempid = Integer.valueOf(whosePar[i]);
					interKey.set(tempid);
					interValue.set(nid + ",S," + orginalTag + "," + kdist + "," + lrdValue);
					context.write(interKey, interValue);
				}
			}
		}
	}

	public static class CalLRDReducer extends Reducer<IntWritable, Text, LongWritable, Text> {
		int K;
		LongWritable outputKey = new LongWritable();
		Text outputValue = new Text();

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			dim = conf.getInt(SQConfig.strDimExpression, -1);
			K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
		}

		/**
		 * (Support area)key: partition id || value: nid, type(S), orgType,
		 * k-distance, lrd, lof
		 * 
		 * @return
		 */
		private MetricObject parseSupportObject(int key, String strInput) {
			int partition_id = key;
			int offset = 0;
			String[] tempSubString = strInput.split(SQConfig.sepStrForRecord);
			Record obj = new Record(Long.valueOf(tempSubString[0]));
			char curTag = tempSubString[1].charAt(0);
			char orgTag = tempSubString[2].charAt(0);
			float kdist = Float.parseFloat(tempSubString[3]);
			float curlrd = -1;
			float curlof = -1;
			curlrd = Float.parseFloat(tempSubString[4]);
			return new MetricObject(partition_id, obj, curTag, orgTag, kdist, curlrd, curlof);
		}

		/**
		 * (Core area)key: partition id || value: nid, type(C), orgType,
		 * k-distance, lrd, lof, whoseSupport, (KNN's nid and dist and kdist and
		 * lrd)
		 * 
		 * @param key
		 * @param strInput
		 * @return
		 */
		private MetricObject parseCoreObject(int key, String strInput, Context context) {

			String[] splitStrInput = strInput.split(SQConfig.sepStrForRecord);
			int partition_id = key;
			int offset = 0;
			Record obj = new Record(Long.valueOf(splitStrInput[0]));
			char curTag = splitStrInput[1].charAt(0);
			char orgTag = splitStrInput[2].charAt(0);

			float curKdist = Float.parseFloat(splitStrInput[3]);
			float curLrd = Float.parseFloat(splitStrInput[4]);
			float curLof = Float.parseFloat(splitStrInput[5]);
			String whoseSupport = splitStrInput[6];
			Map<Long, coreInfoKNNs> knnInDetail = new HashMap<Long, coreInfoKNNs>();
			if(splitStrInput.length <= 6+K){
				System.out.println("Cannot phase: " + strInput);
				return null;
			}
			for (int i = 0; i < K; i++) {
				String[] tempSplit = splitStrInput[7 + i].split(SQConfig.sepSplitForIDDist);
				if (tempSplit.length > 1) {
					long knnid = Long.parseLong(tempSplit[0]);
					float knndist = Float.parseFloat(tempSplit[1]);
					float knnkdist = Float.parseFloat(tempSplit[2]);
					float knnlrd = Float.parseFloat(tempSplit[3]);
					coreInfoKNNs coreInfo = new coreInfoKNNs(knndist, knnkdist, knnlrd);
					knnInDetail.put(knnid, coreInfo);
				} else {
					break;
				}
			}
			return new MetricObject(partition_id, obj, curTag, orgTag, knnInDetail, curKdist, curLrd, curLof,
					whoseSupport);
		}

		/**
		 * find knn for each string in the key.pid format of each value in
		 * values
		 * 
		 */
		@SuppressWarnings("unchecked")
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Vector<MetricObject> coreData = new Vector();
			HashMap<Long, Float> hm_kdistance = new HashMap();
			for (Text value : values) {
				if (value.toString().contains("S")) {
					MetricObject mo = parseSupportObject(key.get(), value.toString());
					hm_kdistance.put(((Record) mo.getObj()).getRId(), mo.getKdist());
				} else if (value.toString().contains("C")) {
					MetricObject mo = parseCoreObject(key.get(), value.toString(), context);
					if(mo!=null){
					coreData.addElement(mo);
					hm_kdistance.put(((Record) mo.getObj()).getRId(), mo.getKdist());
					}
				}
			}
			long begin = System.currentTimeMillis();
			for (MetricObject o_S : coreData) {
				CalLRDForSingleObject(context, o_S, hm_kdistance);
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
		private void CalLRDForSingleObject(Context context, MetricObject o_S, HashMap<Long, Float> hm)
				throws IOException, InterruptedException {

			float lrd_core = 0.0f;
			boolean canCalLRD = true;
			if (o_S.getOrgType() == 'T') {
				for (Map.Entry<Long, coreInfoKNNs> entry : o_S.getKnnMoreDetail().entrySet()) {
					long keyMap = entry.getKey();
					coreInfoKNNs valueMap = entry.getValue();
					float valueDist = valueMap.dist;
					float valueKdistance = valueMap.kdist;
					if (valueKdistance <= 0) {
						if (hm.containsKey(keyMap))
							valueKdistance = hm.get(keyMap);
						else if (valueKdistance < 0) {
							System.out.println("Cannot find.. mayber pruned");
							canCalLRD = false;
							break;
						}
					}
					float temp_reach_dist = Math.max(valueDist, valueKdistance);
					lrd_core += temp_reach_dist;
				}
				if (lrd_core != 0)
					lrd_core = 1.0f / (lrd_core / K * 1.0f);
				o_S.setLrdValue(lrd_core);
				o_S.setOrgType('L');
			}
			if (canCalLRD) {
				String line = "";
				// output format key:nid value: partition id, original type,
				// lrd, lof, whoseSupport,
				// (KNN's nid and lrd)
				line = line + o_S.getPartition_id() + SQConfig.sepStrForRecord + o_S.getOrgType()
						+ SQConfig.sepStrForRecord + o_S.getLrdValue() + SQConfig.sepStrForRecord + o_S.getLofValue()
						+ SQConfig.sepStrForRecord + o_S.getWhoseSupport() + SQConfig.sepStrForRecord;
				for (Map.Entry<Long, coreInfoKNNs> entry : o_S.getKnnMoreDetail().entrySet()) {
					long keyMap = entry.getKey();
					coreInfoKNNs valueMap = entry.getValue();
					line = line + keyMap + SQConfig.sepStrForIDDist + valueMap.lrd + SQConfig.sepStrForRecord;
				}
				line = line.substring(0, line.length() - 1);

				outputValue.set(line);
				outputKey.set(((Record) o_S.getObj()).getRId());
				context.write(outputKey, outputValue);
			} // end if
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "Calculate lrd");
		String strFSName = conf.get("fs.default.name");

		job.setJarByClass(CalLRD.class);
		job.setMapperClass(CalLRDMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setReducerClass(CalLRDReducer.class);
		// job.setNumReduceTasks(0);
		job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.strKdistFinalOutput)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strLRDOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strLRDOutput)));

		/** print job parameter */
		System.err.println("input path: " + conf.get(SQConfig.strKdistFinalOutput));
		System.err.println("output path: " + conf.get(SQConfig.strLRDOutput));
		System.err.println("dataspace: " + conf.get(SQConfig.strMetricSpace));
		System.err.println("metric: " + conf.get(SQConfig.strMetric));
		System.err.println("value of K: " + conf.get(SQConfig.strK));
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, 10));

		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) throws Exception {
		CalLRD callrd = new CalLRD();
		callrd.run(args);
	}
}
