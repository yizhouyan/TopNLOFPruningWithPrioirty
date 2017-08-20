package lof.pruning.firstknn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import lof.pruning.CalLRD;
import lof.pruning.firstknn.CalKdistanceFirstMultiDim.Counters;
import lof.pruning.firstknn.prQuadTree.prQuadLeaf;
import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricKey;
import metricspace.MetricObject;
import metricspace.MetricSpaceUtility;
import metricspace.Record;
import util.PriorityQueue;
import util.SQConfig;

/**
 * @author yizhouyan
 *
 */
public class FirstKNNFinderReducer extends Reducer<IntWritable, Text, LongWritable, Text> {
	/**
	 * The dimension of data
	 */
	private static int num_dims = 2;
	private int[] indexOfIndependentDims;
	private int num_independentDim;
	private int num_correlatedDim;
	private int[] indexOfCorrelatedDims;
	/** The domains. (set by user) */
	private static float[] domains;
	private static int K;
	private IMetricSpace metricSpace = null;
	private IMetric metric = null;
	private MultipleOutputs<LongWritable, Text> mos;
	private float thresholdLof = 10.0f;
	private PriorityQueue topnLOF = new PriorityQueue(PriorityQueue.SORT_ORDER_ASCENDING);
	private int topNNumber = 100;
	private float segmentLength = 50;
	// private int countNumPartition = 0;

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
		mos = new MultipleOutputs<LongWritable, Text>(context);
		Configuration conf = context.getConfiguration();
		readMetricAndMetricSpace(conf);
		/** get configuration from file */
		num_dims = conf.getInt(SQConfig.strDimExpression, 2);

		/** parse files in the cache */
		try {
			URI[] cacheFiles = context.getCacheArchives();
			// read in dim information
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
						currentReader.close();
						currentStream.close();
					}
				}
			} // end for URI

		} catch (IOException ioe) {
			System.err.println("Caught exception while getting cached files");
		}

		domains = new float[2];
		domains[0] = conf.getFloat(SQConfig.strDomainMin, 0.0f);
		domains[1] = conf.getFloat(SQConfig.strDomainMax, 10001.0f);
		K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
		segmentLength = conf.getFloat(SQConfig.strSafePruningZoneSegment, 50);
		thresholdLof = conf.getFloat(SQConfig.strLOFThreshold, 1.0f);
		topNNumber = conf.getInt(SQConfig.strLOFTopNThreshold, 100);
	} // end setup

	private MetricObject parseObject(int key, String strInput) {
		int partition_id = key;
		int offset = 0;
		Object obj = metricSpace.readObject(strInput.substring(offset, strInput.length()), num_dims);
		return new MetricObject(partition_id, obj);
	}

	public static float[] maxOfTwoFloatArray(float[] x, float[] y) {
		float[] newArray = new float[x.length];
		for (int i = 0; i < x.length; i++) {
			newArray[i] = Math.max(x[i], y[i]);
		}
		return newArray;
	}

	/**
	 * default Reduce class.
	 * 
	 * @author Yizhou Yan
	 * @version Dec 25, 2016
	 * @throws InterruptedException
	 */
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// current partition id
		int currentPid = key.get();
//		if (currentPid != 0)
//			return;
		// partition information for current partition and adjacent
		// partition
		HashMap<Integer, float[]> partition_store = new HashMap<Integer, float[]>();
		// save all points in this partition
		ArrayList<MetricObject> pointList = new ArrayList<MetricObject>();

		// System.out.println("Partition:" + currentPid);
		for (Text value : values) {
			if (value.toString().contains("PInfo")) {
				// add to partition information
				// System.out.println("Key: " + key.get() + ",Value: " +
				// value.toString());
				String[] splitStr = value.toString().split(":")[1].split(SQConfig.sepStrForRecord);
				int tempPID = Integer.parseInt(splitStr[0]);
				if (partition_store.containsKey(tempPID)) {
					// update partition_store
					float[] tempPartition = partition_store.get(tempPID);
					for (int i = 0; i < num_dims; i++) {
						tempPartition[2 * i] = Math.min(tempPartition[2 * i], Float.parseFloat(splitStr[2 * i + 1]));
						tempPartition[2 * i + 1] = Math.max(tempPartition[2 * i + 1],
								Float.parseFloat(splitStr[2 * i + 2]));
					}
					partition_store.put(tempPID, tempPartition);
				} else {
					// add to partition_store
					float[] tempPartition = new float[num_dims * 2];
					for (int i = 0; i < num_dims; i++) {
						tempPartition[2 * i] = Float.parseFloat(splitStr[2 * i + 1]);
						tempPartition[2 * i + 1] = Float.parseFloat(splitStr[2 * i + 2]);
					}
					partition_store.put(tempPID, tempPartition);
				}

			} else {
				MetricObject mo = parseObject(key.get(), value.toString());
				// MetricObject mo = parseObject(key.get(), value.toString());
				pointList.add(mo);
			}
		} // end collect data

		// for (Map.Entry<Integer, float[]> e : partition_store.entrySet()) {
		// System.out.println(e.getKey() + "," + e.getValue()[4] + "," +
		// e.getValue()[5] + "," + e.getValue()[6] + ","
		// + e.getValue()[7]);
		// }

		if (pointList.size() < K + 1)
			System.out.println("Less points than K + 1, Do not process");
		else {
			// System.out.println(
			// "Partition: " + partition_store.get(currentPid)[0] + "," +
			// partition_store.get(currentPid)[1] + ","
			// + partition_store.get(currentPid)[2] + "," +
			// partition_store.get(currentPid)[3] + ","
			// + partition_store.get(currentPid)[4] + "," +
			// partition_store.get(currentPid)[5] + ","
			// + partition_store.get(currentPid)[6] + "," +
			// partition_store.get(currentPid)[7]);
			ArrayList<LargeCellStore> leaveNodes = new ArrayList<LargeCellStore>();
			ClosestPair cpObj = new ClosestPair(partition_store.get(currentPid));
			float[] independentCoordinates = new float[num_independentDim * 2];
			for (int i = 0; i < num_independentDim; i++) {
				independentCoordinates[i * 2] = partition_store.get(currentPid)[indexOfIndependentDims[i] * 2];
				independentCoordinates[i * 2 + 1] = partition_store.get(currentPid)[indexOfIndependentDims[i] * 2 + 1];
			}
			// System.out.println("Partition: " + independentCoordinates[0] +
			// "," + independentCoordinates[1] + ","
			// + independentCoordinates[2] + "," + independentCoordinates[3]);

			partitionTreeNode ptn = cpObj.divideAndConquer(pointList, independentCoordinates, indexOfIndependentDims,
					leaveNodes, K, metric, metricSpace);

			// sort by priority
			Collections.sort(leaveNodes, new Comparator<LargeCellStore>() {
				public int compare(LargeCellStore l1, LargeCellStore l2) {
					if (l1.getBucketPriority() > l2.getBucketPriority())
						return -1;
					else if (l1.getBucketPriority() == l2.getBucketPriority())
						return 0;
					else
						return 1;
				}
			});

			// save points that can prune
			HashMap<Long, MetricObject> CanPrunePoints = new HashMap<Long, MetricObject>();

			// save points that can find exact knns
			HashMap<Long, MetricObject> TrueKnnPoints = new HashMap<Long, MetricObject>();
			HashMap<Long, MetricObject> lrdHM = new HashMap<Long, MetricObject>();

			for (int i = 0; i < leaveNodes.size(); i++) {
				leaveNodes.get(i).innerSearchWithEachLargeCell(CanPrunePoints, TrueKnnPoints, lrdHM, K, i, thresholdLof,
						ptn, leaveNodes, topnLOF, topNNumber, indexOfIndependentDims,
						indexOfCorrelatedDims, num_dims, context);
			} // end for (int i = 0; i < leaveNodes.size(); i++)
			
			context.getCounter(Counters.CellPrunedPoints).increment(CanPrunePoints.size());

			String outputKDistPath = context.getConfiguration().get(SQConfig.strKdistanceOutput);
			String outputPPPath = context.getConfiguration().get(SQConfig.strKnnPartitionPlan);

			/**
			 * deal with boundary points also deal with large cells that not
			 * broke up
			 *
			 * bound supporting area for the partition
			 */
			float[] partitionExpand = new float[indexOfIndependentDims.length * 2];
			for (int i = 0; i < indexOfIndependentDims.length * 2; i++) {
				partitionExpand[i] = 0.0f;
			}

			for (int i = 0; i < leaveNodes.size(); i++) {
				if (leaveNodes.get(i).isBreakIntoSmallCells()) {
					// find kNNs within the PR quad tree
					partitionExpand = maxOfTwoFloatArray(partitionExpand,
							leaveNodes.get(i).findKnnsWithinPRTreeOutsideBucket(TrueKnnPoints, leaveNodes, i, ptn,
									independentCoordinates, K, num_dims, domains, indexOfIndependentDims));

				} else if (leaveNodes.get(i).getNumOfPoints() != 0) {
					// else find kNNs within the large cell
					partitionExpand = maxOfTwoFloatArray(partitionExpand,
							leaveNodes.get(i).findKnnsForLargeCellOutsideBucket(TrueKnnPoints, leaveNodes, i, ptn,
									independentCoordinates, K, num_dims, domains, indexOfIndependentDims));
				}
			}
			
			// save those pruned points but need to recompute KNNs
			HashMap<Long, MetricObject> needCalculatePruned = new HashMap<Long, MetricObject>();
			// save those cannot be pruned only by LRD value...
			HashMap<Long, MetricObject> needCalLOF = new HashMap<Long, MetricObject>();
			// need more knn information, maybe knn is pruned...
			HashMap<Long, MetricObject> needRecalLRD = new HashMap<Long, MetricObject>();

			// calculate LRD for points that can calculate
			for (MetricObject mo : TrueKnnPoints.values()) {
				if (mo.getType() == 'T')
					ComputeLRD.CalLRDForSingleObject(mo, TrueKnnPoints, CanPrunePoints, needCalculatePruned, lrdHM,
							needCalLOF, needRecalLRD, thresholdLof, context, leaveNodes, K);
			}

			// for those pruned by cell-based pruning, find kNNs for these
			// points
			for (MetricObject mo : needCalculatePruned.values()) {
				int tempIndex = mo.getIndexOfCPCellInList();
				prQuadLeaf curLeaf = leaveNodes.get(tempIndex).findLeafWithSmallCellIndex(
						leaveNodes.get(tempIndex).getRootForPRTree(), mo.getIndexForSmallCell(),
						indexOfIndependentDims);
				if (!mo.isInsideKNNfind()) {
					leaveNodes.get(tempIndex).findKnnsForOnePointInsideBucket(TrueKnnPoints, mo, curLeaf,
							leaveNodes.get(tempIndex), K, indexOfIndependentDims);
				}
				leaveNodes.get(tempIndex).findKnnsForOnePointOutsideBucket(TrueKnnPoints, mo, leaveNodes,
						leaveNodes.get(tempIndex), ptn, independentCoordinates, K, num_dims, domains,
						indexOfIndependentDims);

			}
			// knn's knn is pruned...
			HashMap<Long, MetricObject> needCalculateLRDPruned = new HashMap<Long, MetricObject>();
			// calculate LRD for some points again....
			for (MetricObject mo : needRecalLRD.values()) {
				ComputeLRD.ReCalLRDForSpecial(context, mo, TrueKnnPoints, needCalculatePruned, lrdHM, needCalLOF,
						needCalculateLRDPruned, thresholdLof, K);
			}

			// (needs implementation) calculate LRD for points that needs
			// Calculate LRD (deal with needCalculateLRDPruned)
			for (MetricObject mo : needCalculateLRDPruned.values()) {
				float lrd_core = 0.0f;
				boolean canCalLRD = true;
				long[] KNN_moObjectsID = mo.getPointPQ().getValueSet();
				float[] moDistToKNN = mo.getPointPQ().getPrioritySet();

				for (int i = 0; i < KNN_moObjectsID.length; i++) {
					long knn_mo = KNN_moObjectsID[i];
					// first point out which large cell it is in
					// (tempIndexX, tempIndexY)
					float kdistknn = 0.0f;
					if (TrueKnnPoints.containsKey(knn_mo))
						kdistknn = TrueKnnPoints.get(knn_mo).getKdist();
					else if (CanPrunePoints.containsKey(knn_mo) && (!needCalculatePruned.containsKey(knn_mo))) {
						MetricObject newKnnFind = CanPrunePoints.get(knn_mo);
						int tempIndex = newKnnFind.getIndexOfCPCellInList();
						prQuadLeaf curLeaf = leaveNodes.get(tempIndex).findLeafWithSmallCellIndex(
								leaveNodes.get(tempIndex).getRootForPRTree(), newKnnFind.getIndexForSmallCell(),
								indexOfIndependentDims);
						if (!newKnnFind.isInsideKNNfind()) {
							leaveNodes.get(tempIndex).findKnnsForOnePointInsideBucket(TrueKnnPoints, newKnnFind,
									curLeaf, leaveNodes.get(tempIndex), K, indexOfIndependentDims);
						}
						leaveNodes.get(tempIndex).findKnnsForOnePointOutsideBucket(TrueKnnPoints, newKnnFind,
								leaveNodes, leaveNodes.get(tempIndex), ptn, independentCoordinates, K,
								num_dims, domains, indexOfIndependentDims);
						if (TrueKnnPoints.containsKey(knn_mo))
							kdistknn = TrueKnnPoints.get(knn_mo).getKdist();
						else {
							canCalLRD = false;
							break;
						}
					} else {
						canCalLRD = false;
						break;
					}
					float temp_reach_dist = Math.max(moDistToKNN[i], kdistknn);
					lrd_core += temp_reach_dist;
					// System.out.println("Found KNNs for pruning point: " +
					// mo.getKdist());
				}
				if (canCalLRD) {
					lrd_core = 1.0f / (lrd_core / K * 1.0f);
					mo.setLrdValue(lrd_core);
					mo.setType('L');
					lrdHM.put(((Record) mo.getObj()).getRId(), mo);
				}
			} // end calculate LRD for pruned points
				////////////////////////////////////////////////////////////////////////////////////////
				// calculate LOF for points that can calculate
			for (MetricObject mo : needCalLOF.values()) {
				ComputeLOF.CalLOFForSingleObject(context, mo, lrdHM, K, thresholdLof);
				if (mo.getType() == 'O' && mo.getLofValue() > thresholdLof) {
					float tempLofValue = mo.getLofValue();
					if (topnLOF.size() < topNNumber) {
						topnLOF.insert(metricSpace.getID(mo.getObj()), tempLofValue);

					} else if (tempLofValue > topnLOF.getPriority()) {
						topnLOF.pop();
						topnLOF.insert(metricSpace.getID(mo.getObj()), tempLofValue);
						if (thresholdLof < topnLOF.getPriority())
							thresholdLof = topnLOF.getPriority();
						// System.out.println("Threshold updated: " +
						// thresholdLof);
					}
				}
			}
			if (topnLOF.size() == topNNumber && thresholdLof < topnLOF.getPriority())
				thresholdLof = topnLOF.getPriority();
			// compute safe pruning zone
//			SafePruningZone comSafePruning = new SafePruningZone(num_dims, segmentLength, K, metric);
//			comSafePruning.computeSafePruningPoints(pointList, partition_store, currentPid);
			
			SafePruningZoneOPT comSafePruning = new SafePruningZoneOPT(num_dims, segmentLength, K, metric);
			comSafePruning.computeSafePruningPoints(pointList, partition_store, currentPid, ptn, indexOfIndependentDims);
//			int countOutputPoint  = 0;
//			for (MetricObject mo : pointList) {
//				if (mo.isOthersSupport())
//					countOutputPoint++;
//			}
//			System.out.println("Pruned points: " + countOutputPoint);
			// output data points and calculate partition bound
			for (int i = 0; i < leaveNodes.size(); i++) {
				for (MetricObject o_R : leaveNodes.get(i).getListOfPoints()) {
					if(!o_R.isOthersSupport() && (o_R.isCanPrune() || o_R.getType() == 'O' ))
						continue;
					else{
						context.getCounter(Counters.FinalOutputPoints).increment(1);
						outputMultipleTypeData(o_R, outputKDistPath, lrdHM, needCalculatePruned, context);
					}
				}
			}

			// // output partition plan
			String str = currentPid + SQConfig.sepStrForRecord;
			for (int i = 0; i < num_independentDim; i++) {
				str += independentCoordinates[2 * i] + SQConfig.sepStrForRecord
						+ independentCoordinates[2 * i + 1] + SQConfig.sepStrForRecord;
			}
			for (int i = 0; i < num_independentDim; i++) {
				str += partitionExpand[2 * i] + SQConfig.sepStrForRecord + partitionExpand[2 * i + 1]
						+ SQConfig.sepStrForRecord;
			}
			str = str.substring(0, str.length() - 1);
			mos.write(new LongWritable(1), new Text(str), outputPPPath + "/pp_" + context.getTaskAttemptID());
			System.err.println("computation finished");
		}
	} // end reduce

	/**
	 * output different types of data points in multiple files
	 * 
	 * @param context
	 * @param o_R
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void outputMultipleTypeData(MetricObject o_R, String outputKDistPath, HashMap<Long, MetricObject> lrdHM,
			HashMap<Long, MetricObject> needCalculatePruned, Context context) throws IOException, InterruptedException {
		// output format key:nid value: point value, partition id,canPrune,
		// tag,
		// (KNN's nid and dist|kdist|lrd for that point),k-distance, lrd,
		// lof,
		LongWritable outputKey = new LongWritable();
		Text outputValue = new Text();

		String line = "";
		line = line + o_R.getPartition_id() + SQConfig.sepStrForRecord;
		if (o_R.isCanPrune()) {
			line = line + "T";
			line = line + SQConfig.sepStrForRecord + o_R.getType() + SQConfig.sepStrForRecord;
			line = line + o_R.getKdist() + SQConfig.sepStrForRecord + o_R.getLrdValue() + SQConfig.sepStrForRecord
					+ o_R.getLofValue();
		} else {
			line = line + "F";
			line = line + SQConfig.sepStrForRecord + o_R.getType() + SQConfig.sepStrForRecord;
			long[] KNN_moObjectsID = o_R.getPointPQ().getValueSet();
			float[] moDistToKNN = o_R.getPointPQ().getPrioritySet();
			for (int i = 0; i < moDistToKNN.length; i++) {
				long keyMap = KNN_moObjectsID[i];
				float valueMap = moDistToKNN[i];
				float kdistForKNN = 0.0f;
				float lrdForKNN = 0.0f;
				if (lrdHM.containsKey(keyMap)) {
					kdistForKNN = lrdHM.get(keyMap).getKdist();
					lrdForKNN = lrdHM.get(keyMap).getLrdValue();
				} else if (needCalculatePruned.containsKey(keyMap)) {
					kdistForKNN = needCalculatePruned.get(keyMap).getKdist();
				}
				line = line + keyMap + SQConfig.sepStrForIDDist + valueMap + SQConfig.sepStrForIDDist + kdistForKNN
						+ SQConfig.sepStrForIDDist + lrdForKNN + SQConfig.sepStrForRecord;
				
			}
			if(o_R.getType() == 'F' && o_R.getKdist() < 0){
//				System.err.println("Point:" + ((Record) o_R.getObj()).getRId() + ", Size of kNN: " + o_R.getPointPQ().size());
			}
			line = line + o_R.getKdist() + SQConfig.sepStrForRecord + o_R.getLrdValue() + SQConfig.sepStrForRecord
					+ o_R.getLofValue();
		}
		outputKey.set(((Record) o_R.getObj()).getRId());
		outputValue.set(((Record) o_R.getObj()).dimToString() + SQConfig.sepStrForRecord + line);
		mos.write(outputKey, outputValue, outputKDistPath + "/kdist_" + context.getTaskAttemptID());
		// context.write(outputKey, outputValue);
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		// output top n LOF value
		String outputTOPNPath = context.getConfiguration().get(SQConfig.strKnnFirstRoundTopN);
		while (topnLOF.size() > 0) {
			mos.write(new LongWritable(topnLOF.getValue()), new Text(topnLOF.getPriority() + ""),
					outputTOPNPath + "/topn_" + context.getTaskAttemptID());
			topnLOF.pop();
		}
		mos.close();
	}

}