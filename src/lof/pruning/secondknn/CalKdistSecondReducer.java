package lof.pruning.secondknn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import lof.pruning.secondknn.CalKdistanceSecond.Counters;
import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricObjectMore;
import metricspace.MetricSpaceUtility;
import metricspace.Record;
import metricspace.coreInfoKNNs;
import sampling.CellStore;
import util.SQConfig;

public class CalKdistSecondReducer extends Reducer<IntWritable, Text, LongWritable, Text> {
	/**
	 * The dimension of data (set by user, now only support dimension of 2, if
	 * change to 3 or more, has to change some codes)
	 */
	private static int num_dims = 2;
	private static int K;
	private IMetricSpace metricSpace = null;
	private IMetric metric = null;
	private static float[][] partition_store;
	/** The domains. (set by user) */
	private static float[] domains;

	private static int num_partitions;

	private int[] indexOfIndependentDims;
	private int num_independentDim;
	private int num_correlatedDim;
	private int[] indexOfCorrelatedDims;
	private static int maxLimitSupportingArea;

	// private static float thresholdLof = 0.0f;
	// private PriorityQueue topnLOF = new
	// PriorityQueue(PriorityQueue.SORT_ORDER_ASCENDING);
	// private int topNNumber = 100;

	/**
	 * get MetricSpace and metric from configuration
	 * 
	 * @param conf
	 * @throws IOException
	 */
	private void readMetricAndMetricSpace(Configuration conf) throws IOException {
		try {
			metricSpace = MetricSpaceUtility.getMetricSpace(conf.get(SQConfig.strMetricSpace));
			metric = MetricSpaceUtility.getMetric(conf.get(SQConfig.strMetric));
			metricSpace.setMetric(metric);
		} catch (InstantiationException e) {
			throw new IOException("InstantiationException");
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new IOException("IllegalAccessException");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IOException("ClassNotFoundException");
		}
	}

	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		readMetricAndMetricSpace(conf);
		/** get configuration from file */
		domains = new float[2];
		domains[0] = conf.getFloat(SQConfig.strDomainMin, 0.0f);
		domains[1] = conf.getFloat(SQConfig.strDomainMax, 10001.0f);
		num_dims = conf.getInt(SQConfig.strDimExpression, 2);
		K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
		maxLimitSupportingArea = conf.getInt(SQConfig.strMaxLimitSupportingArea, 5000);

		// topNNumber = conf.getInt(SQConfig.strLOFThreshold, 100);
		try {
			URI[] cacheFiles = context.getCacheArchives();

			if (cacheFiles == null || cacheFiles.length < 3) {
				System.out.println("not enough cache files");
				return;
			}

			// first read in dim information
			for (URI path : cacheFiles) {
				String filename = path.toString();
				FileSystem fs = FileSystem.get(conf);

				FileStatus[] stats = fs.listStatus(new Path(filename));
				for (int i = 0; i < stats.length; ++i) {
					if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("dc-r")) {
						System.out.println("Reading dim correlation from " + stats[i].getPath().toString());
						FSDataInputStream currentStream;
						BufferedReader currentReader;
						currentStream = fs.open(stats[i].getPath());
						currentReader = new BufferedReader(new InputStreamReader(currentStream));

						// get number of independent dims
						num_independentDim = Integer.parseInt(currentReader.readLine());
						num_correlatedDim = num_dims - num_independentDim;
						indexOfIndependentDims = new int[num_independentDim];
						indexOfCorrelatedDims = new int[num_correlatedDim];

						// get independent dims
						String independentDimString = currentReader.readLine();
						String[] independentDimSplits = independentDimString.split(",");
						for (int ii = 0; ii < independentDimSplits.length; ii++) {
							indexOfIndependentDims[ii] = Integer.parseInt(independentDimSplits[ii]);
						}

						// get correlated dims
						int iterateOfIndependentDims = 0;
						int iterateOfCorrelatedDims = 0;
						for (int ii = 0; ii < num_dims; ii++) {
							if (ii == indexOfIndependentDims[Math.min(iterateOfIndependentDims,
									num_independentDim - 1)])
								iterateOfIndependentDims++;
							else {
								indexOfCorrelatedDims[iterateOfCorrelatedDims++] = ii;
								System.out.println("Correlated Dim Add: " + ii);
							}
						}
						// get partition number
						num_partitions = Integer.parseInt(currentReader.readLine());
						partition_store = new float[num_partitions][num_dims * 4];
						currentReader.close();
						currentStream.close();
					}
				}
			} // end for URI

			for (URI path : cacheFiles) {
				String filename = path.toString();
				FileSystem fs = FileSystem.get(conf);

				FileStatus[] stats = fs.listStatus(new Path(filename));
				for (int i = 0; i < stats.length; ++i) {
					if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("pp_")) {
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
							int tempid = Integer.parseInt(splitsStr[0]);
							for (int j = 0; j < num_independentDim * 2; j++) {
								partition_store[tempid][j] = Float.parseFloat(splitsStr[j + 1]);
							}
							for (int j = num_independentDim * 2; j < num_independentDim * 4; j++) {
								partition_store[tempid][j] = Math.min(Float.parseFloat(splitsStr[j + 1]),
										maxLimitSupportingArea);
							}
						}
						currentReader.close();
						currentStream.close();
					}
				} // end for (int i = 0; i < stats.length; ++i)
			} // end for (URI path : cacheFiles)

		} catch (IOException ioe) {
			System.err.println("Caught exception while getting cached files");
		}
	}

	/**
	 * parse objects in supporting area key: partition id value: point id, point
	 * information(2-d), tag(S)， tag(FTLO), kdist, lrd, lof
	 * 
	 * @param key:
	 *            partition id
	 * @param strInput:
	 *            point id, point information(2-d), tag(S),tag(FTLO), kdist,
	 *            lrd, lof
	 * @return
	 */
	private MetricObjectMore parseSupportObject(int key, String strInput) {
		// System.out.println(strInput);
		int partition_id = key;
		int offset = 0;
		Object obj = metricSpace.readObject(strInput, num_dims);

		String[] tempSubString = strInput.split(SQConfig.sepStrForRecord);
		char curTag = tempSubString[num_dims + 1].charAt(0);
		if (curTag != 'S')
			System.out.println("Error Support area: " + strInput);
		char orgTag = tempSubString[num_dims + 2].charAt(0);
		float kdist = Float.parseFloat(tempSubString[num_dims + 3]);
		float curlrd = -1;
		// if (tempSubString.length > num_dims + 4)
		curlrd = Float.parseFloat(tempSubString[num_dims + 4]);
		return new MetricObjectMore(partition_id, obj, curTag, orgTag, kdist, curlrd, -1);
	}

	/**
	 * parse objects in core area key: partition id value: point id, point
	 * information(2-d), tag(C), tag(FTLO), knns,kdist,lrd,lof,whoseSupport
	 * 
	 * @param key
	 * @param strInput
	 * @return
	 */
	private MetricObjectMore parseCoreObject(int key, String strInput, Context context) {
		// System.out.println(strInput);
		String[] splitStrInput = strInput.split(SQConfig.sepStrForRecord);
		int partition_id = key;
		int offset = 0;
		Object obj = metricSpace.readObject(strInput, num_dims);
		// System.out.println(((Record)obj).dimToString());
		char curTag = splitStrInput[num_dims + 1].charAt(0);
		if (curTag != 'C')
			System.out.println("Error Core area: " + strInput);
		char orgTag = splitStrInput[num_dims + 2].charAt(0);
		// knns and kdist, lrd

		Map<Long, coreInfoKNNs> knnInDetail = new HashMap<Long, coreInfoKNNs>();
		int countKnns = 0;
		for (int i = 0; i < K; i++) {
			String[] tempSplit = splitStrInput[num_dims + 3 + i].split(SQConfig.sepSplitForIDDist);
			if (tempSplit.length > 1) {
				long knnid = Long.parseLong(tempSplit[0]);
				float knndist = Float.parseFloat(tempSplit[1]);
				float knnkdist = Float.parseFloat(tempSplit[2]);
				float knnlrd = Float.parseFloat(tempSplit[3]);
				coreInfoKNNs coreInfo = new coreInfoKNNs(knndist, knnkdist, knnlrd);
				knnInDetail.put(knnid, coreInfo);
				countKnns++;
			} else {
				break;
			}
		}
		float curKdist = Float.parseFloat(splitStrInput[num_dims + 3 + countKnns]);
		float curLrd = Float.parseFloat(splitStrInput[num_dims + 4 + countKnns]);
		String whoseSupport = "";
		if (splitStrInput.length > num_dims + 6 + countKnns)
			whoseSupport = splitStrInput[num_dims + 6 + countKnns];
		return new MetricObjectMore(partition_id, obj, curTag, orgTag, knnInDetail, curKdist, curLrd, -1,
				whoseSupport);
	}

	/**
	 * default Reduce class.
	 * 
	 * @author Yizhou Yan
	 * @version Dec 31, 2015
	 * @throws InterruptedException
	 */
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		float[] newLargeCoor = new float[num_independentDim * 2];
		for (int i = 0; i < num_independentDim; i++) {
			newLargeCoor[2 * i] = partition_store[key.get()][2 * i]
					- partition_store[key.get()][2 * num_independentDim + 2 * i];
			newLargeCoor[2 * i + 1] = partition_store[key.get()][2 * i + 1]
					+ partition_store[key.get()][2 * num_independentDim + 2 * i + 1];
		}
		// build up a large cell store so that can build a quadtree
		LargeCellStoreMore lcs = new LargeCellStoreMore(newLargeCoor, metric, metricSpace);
		HashMap<Long, MetricObjectMore> supportingPoints = new HashMap<Long, MetricObjectMore>();
		HashMap<Long, MetricObjectMore> corePoints = new HashMap<Long, MetricObjectMore>();
		int countSupporting = 0;
		// int countCorePoints = 0;
		boolean moreSupport = true;
		for (Text value : values) {
			if (value.toString().contains("S")) {
				MetricObjectMore mo = parseSupportObject(key.get(), value.toString());
				countSupporting++;
				if (moreSupport) {
					supportingPoints.put(((Record) mo.getObj()).getRId(), mo);
					lcs.addPoints(mo);
					if (countSupporting >= 8000000) {
						// System.out.println("Supporting Larger than 100w
						// ");
						moreSupport = false;
					}
				}
			} else if (value.toString().contains("C")) {
				MetricObjectMore mo = parseCoreObject(key.get(), value.toString(), context);
				// if(mo.getKnnMoreDetail().size()!=K)
				// context.getCounter(Counters.InputLessK).increment(1);
				lcs.addPoints(mo);
				corePoints.put(((Record) mo.getObj()).getRId(), mo);
				// countCorePoints++;
			}
		} // end for collect data
		context.getCounter(Counters.CountDuplicatePoints).increment(countSupporting);
		if (!moreSupport) {
			System.out.println("Too many points in one reducer: " + countSupporting);
		}

		// build up the quad tree
		lcs.seperateLargeNoPrune(K, indexOfIndependentDims, indexOfCorrelatedDims);

		// // search knn for core points
		if (lcs.isBreakIntoSmallCells()) {
			for (MetricObjectMore mo : corePoints.values()) {
				if (mo.getOrgType() == 'F') {
					prQuadLeafMore curLeaf = lcs.findLeafWithSmallCellIndex(lcs.getRootForPRTree(),
							mo.getIndexForSmallCell(), indexOfIndependentDims);
					// then find kNNs for this point (through the tree)
					lcs.findKnnsForOnePointSecondTime(mo, curLeaf, lcs, supportingPoints, partition_store[key.get()], K,
							num_dims, domains, indexOfIndependentDims, context);
				}
			} // end for
		} else if (corePoints.size() != 0) {
			for (MetricObjectMore mo : corePoints.values()) {
				if (mo.getOrgType() == 'F') {
					lcs.findKnnsForOnePointInLargeCellSecondTime(mo, supportingPoints, partition_store[key.get()], K,
							num_dims, domains, indexOfIndependentDims, context);
				}
			}
		}

		// output core area points
		for (MetricObjectMore o_S : corePoints.values()) {
			// output data point
			// output format key:nid
			// value: partition id, point value, k-distance, (KNN's nid
			// and dist),tag, whoseSupport
			LongWritable outputKey = new LongWritable();
			Text outputValue = new Text();
			String line = "";
			line = line + o_S.getPartition_id() + SQConfig.sepStrForRecord + o_S.getOrgType() + SQConfig.sepStrForRecord
					+ o_S.getKdist() + SQConfig.sepStrForRecord + o_S.getLrdValue() + SQConfig.sepStrForRecord + o_S.getWhoseSupport() + SQConfig.sepStrForRecord;
			for (Map.Entry<Long, coreInfoKNNs> entry : o_S.getKnnMoreDetail().entrySet()) {
				long keyMap = entry.getKey();
				coreInfoKNNs valueMap = entry.getValue();
				line = line + keyMap + SQConfig.sepStrForIDDist + valueMap.dist + SQConfig.sepStrForIDDist
						+ valueMap.kdist + SQConfig.sepStrForIDDist + valueMap.lrd + SQConfig.sepStrForRecord;
			}
			line = line.substring(0, line.length() - 1);
			outputKey.set(((Record) o_S.getObj()).getRId());
			outputValue.set(line);
			context.write(outputKey, outputValue);
		}
	}

	// private boolean CalLRDForSingleObject(MetricObject o_S,
	// HashMap<Long, MetricObject> lrdHM, float threshold, Context context)
	// throws IOException, InterruptedException {
	// float lrd_core = 0.0f;
	// float reachdistMax = 0.0f;
	//
	// boolean canLRD = true;
	// int countPruned = 0;
	// for (Map.Entry<Long, coreInfoKNNs> entry :
	// o_S.getKnnMoreDetail().entrySet()) {
	// long temp_kNNKey = entry.getKey();
	// coreInfoKNNs temp_core_info = entry.getValue();
	//
	// if(temp_core_info.kdist == -1){
	// float temp_reach_dist = Math.max(temp_core_info.dist,
	// o_S.getKdist());
	// }
	// else
	// return false;
	// if (!TrueKnnPoints.containsKey(temp_kNNKey)) {
	// if (CanPrunePoints.containsKey(temp_kNNKey)) {
	// needCalculatePruned.put(temp_kNNKey,
	// CanPrunePoints.get(temp_kNNKey));
	// canLRD = false;
	// continue;
	// } else
	// return false;
	// }
	//
	// lrd_core += temp_reach_dist;
	// }
	// if (!canLRD) {
	// needRecalLRD.put(((Record) o_S.getObj()).getRId(), o_S);
	// return false;
	// }
	// lrd_core = 1.0f / (lrd_core / K * 1.0f);
	// o_S.setLrdValue(lrd_core);
	// o_S.setType('L');
	//
	// lrdHM.put(((Record) o_S.getObj()).getRId(), o_S);
	// // System.out.println("LRD-----" + lrd_core);
	// context.getCounter(Counters.LRDPrunedPoints).increment(countPruned);
	// return true;
	// }

}
