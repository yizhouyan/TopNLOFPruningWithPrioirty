package lof.pruning;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import lof.pruning.LargeCellBasedKnnFind.Counters;
import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricObject;
import metricspace.MetricSpaceUtility;
import metricspace.Record;
import metricspace.SafeArea;
import sampling.CellStore;
import util.SQConfig;

public class LargeCellBasedKnnFind {
	/**
	 * default Map class.
	 *
	 * @author Yizhou Yan
	 * @version July 7, 2016
	 */

	/** number of object pairs to be computed */
	static enum Counters {
		CellPrunedPoints, LRDPrunedPoints, FinalPrunedPoints,
	}

	public static class CellBasedKNNFinderMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		/**
		 * The dimension of data (set by user, now only support dimension of 2,
		 * if change to 3 or more, has to change some codes)
		 */
		private static int num_dims = 2;
		/**
		 * number of small cells per dimension: when come with a node, map to a
		 * range (divide the domain into small_cell_num_per_dim) (set by user)
		 */
		public static int cell_num = 501;

		/** The domains. (set by user) */
		private static float[][] domains;
		/** size of each small buckets */
		private static int smallRange;
		/**
		 * block list, which saves each block's info including start & end
		 * positions on each dimension. print for speed up "mapping"
		 */
		private static float[][] partition_store;
		/** save each small buckets. in order to speed up mapping process */
		private static CellStore[][] cell_store;
		/**
		 * Number of desired partitions in each dimension (set by user), for
		 * Data Driven partition
		 */
		private static int[] di_numBuckets;

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			/** get configuration from file */
			num_dims = conf.getInt(SQConfig.strDimExpression, 2);
			cell_num = conf.getInt(SQConfig.strNumOfSmallCells, 501);
			domains = new float[num_dims][2];
			domains[0][0] = domains[1][0] = conf.getFloat(SQConfig.strDomainMin, 0.0f);
			domains[0][1] = domains[1][1] = conf.getFloat(SQConfig.strDomainMax, 10001.0f);
			smallRange = (int) Math.ceil((domains[0][1] - domains[0][0]) / cell_num);
			cell_store = new CellStore[cell_num][cell_num];
			di_numBuckets = new int[num_dims];
			for (int i = 0; i < num_dims; i++) {
				di_numBuckets[i] = conf.getInt(SQConfig.strNumOfPartitions, 2);
			}
			partition_store = new float[di_numBuckets[0] * di_numBuckets[1] + 1][num_dims * 2];
			/** parse files in the cache */
			try {
				URI[] cacheFiles = context.getCacheArchives();

				if (cacheFiles == null || cacheFiles.length < 2) {
					System.out.println("not enough cache files");
					return;
				}
				for (URI path : cacheFiles) {
					String filename = path.toString();
					FileSystem fs = FileSystem.get(conf);

					FileStatus[] stats = fs.listStatus(new Path(filename));
					for (int i = 0; i < stats.length; ++i) {
						if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("pp")) {
							// System.out.println("Reading partition plan from "
							// + stats[i].getPath().toString());
							FSDataInputStream currentStream;
							BufferedReader currentReader;
							currentStream = fs.open(stats[i].getPath());
							currentReader = new BufferedReader(new InputStreamReader(currentStream));
							String line;
							while ((line = currentReader.readLine()) != null) {
								/** parse line */

								String[] splitsStr = line.split(SQConfig.sepStrForRecord);
								int tempid = Integer.parseInt(splitsStr[0]);
								for (int j = 0; j < num_dims * 2; j++) {
									partition_store[tempid][j] = Float.parseFloat(splitsStr[j + 1]);
								}
								// System.out.println("partitionplan : "+
								// partition_store[tempid][3]);
							}
						} else if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("part")) {
							// System.out.println("Reading cells for partitions
							// from " + stats[i].getPath().toString());
							FSDataInputStream currentStream;
							BufferedReader currentReader;
							currentStream = fs.open(stats[i].getPath());
							currentReader = new BufferedReader(new InputStreamReader(currentStream));
							String line;
							while ((line = currentReader.readLine()) != null) {
								/** parse line */
								String[] items = line.split(SQConfig.sepStrForRecord);
								if (items.length == 6) {
									int x_1 = (int) (Float.valueOf(items[0]) / smallRange);
									int y_1 = (int) (Float.valueOf(items[2]) / smallRange);
									cell_store[x_1][y_1] = new CellStore(x_1 * smallRange, (x_1 + 1) * smallRange,
											(y_1) * smallRange, (y_1 + 1) * smallRange);
									cell_store[x_1][y_1].core_partition_id = Integer.valueOf(items[4].substring(2));
								}
							}
						} // end else if
					} // end for (int i = 0; i < stats.length; ++i)
				} // end for (URI path : cacheFiles)

			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files");
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Variables
			float[] crds = new float[num_dims]; // coordinates of one input
												// data
			// parse raw input data into coordinates/crds
			for (int i = 1; i < num_dims + 1; i++) {
				crds[i - 1] = Float.parseFloat(value.toString().split(SQConfig.sepStrForRecord)[i]);
			}
			// find which cell the point in
			int x_cellstore = (int) (Math.floor(crds[0] / (smallRange + Float.MIN_VALUE)));
			int y_cellstore = (int) (Math.floor(crds[1] / (smallRange + Float.MIN_VALUE)));
			// partition id = core area partition id
			int blk_id = cell_store[x_cellstore][y_cellstore].core_partition_id;
			context.write(new IntWritable(blk_id), value);
		}
	}

	/**
	 * @author yizhouyan
	 *
	 */
	public static class CellBasedKNNFinderReducer extends Reducer<IntWritable, Text, LongWritable, Text> {
		/**
		 * The dimension of data (set by user, now only support dimension of 2,
		 * if change to 3 or more, has to change some codes)
		 */
		private static int num_dims = 2;
		/** The domains. (set by user) */
		private static float[][] domains;
		/**
		 * block list, which saves each block's info including start & end
		 * positions on each dimension. print for speed up "mapping"
		 */
		private static float[][] partition_store;
		/**
		 * Number of desired partitions in each dimension (set by user), for
		 * Data Driven partition
		 */
		private static int[] di_numBuckets;
		private static int K;
		private IMetricSpace metricSpace = null;
		private IMetric metric = null;
		private MultipleOutputs<LongWritable, Text> mos;
		private float thresholdLof = 10.0f;
		private PriorityQueue topnLOF = new PriorityQueue(PriorityQueue.SORT_ORDER_ASCENDING);
		private int topNNumber = 100;
		private int countNumPartition = 0;

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
			domains = new float[num_dims][2];
			domains[0][0] = domains[1][0] = conf.getFloat(SQConfig.strDomainMin, 0.0f);
			domains[0][1] = domains[1][1] = conf.getFloat(SQConfig.strDomainMax, 10001.0f);
			di_numBuckets = new int[num_dims];
			for (int i = 0; i < num_dims; i++) {
				di_numBuckets[i] = conf.getInt(SQConfig.strNumOfPartitions, 2);
			}
			partition_store = new float[di_numBuckets[0] * di_numBuckets[1] + 1][num_dims * 2];
			K = Integer.valueOf(conf.get(SQConfig.strK, "1"));

			thresholdLof = conf.getFloat(SQConfig.strLOFThreshold, 1.0f);
			topNNumber = conf.getInt(SQConfig.strLOFTopNThreshold, 100);

			/** parse files in the cache */
			try {
				URI[] cacheFiles = context.getCacheArchives();

				if (cacheFiles == null || cacheFiles.length < 2) {
					System.out.println("not enough cache files");
					return;
				}
				for (URI path : cacheFiles) {
					String filename = path.toString();
					FileSystem fs = FileSystem.get(conf);

					FileStatus[] stats = fs.listStatus(new Path(filename));
					for (int i = 0; i < stats.length; ++i) {
						if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("pp")) {
							FSDataInputStream currentStream;
							BufferedReader currentReader;
							currentStream = fs.open(stats[i].getPath());
							currentReader = new BufferedReader(new InputStreamReader(currentStream));
							String line;
							while ((line = currentReader.readLine()) != null) {
								/** parse line */

								String[] splitsStr = line.split(SQConfig.sepStrForRecord);
								int tempid = Integer.parseInt(splitsStr[0]);
								for (int j = 0; j < num_dims * 2; j++) {
									partition_store[tempid][j] = Float.parseFloat(splitsStr[j + 1]);
								}
							}
						}
					} // end for (int i = 0; i < stats.length; ++i)
				} // end for (URI path : cacheFiles)

			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files");
			}
		}

		private MetricObject parseObject(int key, String strInput) {
			int partition_id = key;
			int offset = 0;
			Object obj = metricSpace.readObject(strInput.substring(offset, strInput.length()), num_dims);
			return new MetricObject(partition_id, obj);
		}

		public static float[] maxOfTwoFloatArray(float[] x, float[] y) {
			float[] newArray = { Math.max(x[0], y[0]), Math.max(x[1], y[1]), Math.max(x[2], y[2]),
					Math.max(x[3], y[3]) };
			return newArray;
		}

		/**
		 * default Reduce class.
		 * 
		 * @author Yizhou Yan
		 * @version Aug 24, 2016
		 * @throws InterruptedException
		 */
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// current partition id
			int currentPid = key.get();
			ArrayList<MetricObject> pointList = new ArrayList<MetricObject>();
			for (Text value : values) {
				MetricObject mo = parseObject(key.get(), value.toString());
				pointList.add(mo);
			}
			if (pointList.size() < K + 1)
				System.out.println("Less points than K + 1");
			else {
				// save points that can prune
				HashMap<Long, MetricObject> CanPrunePoints = new HashMap<Long, MetricObject>();

				ArrayList<LargeCellStore> leaveNodes = new ArrayList<LargeCellStore>();
				SafeArea sa = new SafeArea(partition_store[currentPid]);
				partitionTreeNode ptn = ClosestPair.divideAndConquer(pointList, partition_store[currentPid], leaveNodes,
						sa, K, metric, metricSpace);

				float[] safeAbsArea = sa.getExtendAbsSize();
				float[] safeArea = {
						Math.min(partition_store[currentPid][0] + safeAbsArea[0], partition_store[currentPid][1]),
						Math.max(partition_store[currentPid][0], partition_store[currentPid][1] - safeAbsArea[1]),
						Math.min(partition_store[currentPid][2] + safeAbsArea[2], partition_store[currentPid][3]),
						Math.max(partition_store[currentPid][3] - safeAbsArea[3], partition_store[currentPid][2]) };
				for (int i = 0; i < leaveNodes.size(); i++) {
					if (areaInsideSafeArea(safeArea, leaveNodes.get(i).getCoordinates())) {
						leaveNodes.get(i).setSafeArea(true);
					}
				}

				for (int i = 0; i < leaveNodes.size(); i++) {
					if (leaveNodes.get(i).getNumOfPoints() == 0) {
						continue;
					} else if (leaveNodes.get(i).getNumOfPoints() > K * 5) {
						leaveNodes.get(i).seperateToSmallCells(CanPrunePoints, i, thresholdLof, K, safeArea, ptn,
								partition_store[currentPid]);
						if (!leaveNodes.get(i).isBreakIntoSmallCells() && leaveNodes.get(i).getNumOfPoints() > K * 20) {
							leaveNodes.get(i).seperateLargeNoPrune(K, i, safeArea);
						}
						// context.getCounter(Counters.InPrunedList).increment(leaveNodes.get(i).getNumOfPoints());
					}
					// else{
					// context.getCounter(Counters.NoInPrunedList).increment(leaveNodes.get(i).getNumOfPoints());
					// }
				}
				context.getCounter(Counters.CellPrunedPoints).increment(CanPrunePoints.size());

				countNumPartition++;
				System.out.println("Statistics: The " + countNumPartition + " th partition, " + thresholdLof + ","
						+ CanPrunePoints.size());
				// save points that can find exact knns
				HashMap<Long, MetricObject> TrueKnnPoints = new HashMap<Long, MetricObject>();
				String outputKDistPath = context.getConfiguration().get(SQConfig.strKdistanceOutput);
				String outputPPPath = context.getConfiguration().get(SQConfig.strKnnPartitionPlan);

				/**
				 * deal with boundary points also deal with large cells that not
				 * broke up
				 *
				 * bound supporting area for the partition
				 */
				float[] partitionExpand = { 0.0f, 0.0f, 0.0f, 0.0f };
				for (int i = 0; i < leaveNodes.size(); i++) {
					if (leaveNodes.get(i).isBreakIntoSmallCells()) {
						// find kNNs within the PR quad tree
						partitionExpand = maxOfTwoFloatArray(partitionExpand, leaveNodes.get(i).findKnnsWithinPRTree(
								TrueKnnPoints, leaveNodes, i, ptn, partition_store[currentPid], K, num_dims, domains));

					} else if (leaveNodes.get(i).getNumOfPoints() != 0) {
						// else find kNNs within the large cell
						partitionExpand = maxOfTwoFloatArray(partitionExpand, leaveNodes.get(i).findKnnsForLargeCell(
								TrueKnnPoints, leaveNodes, i, ptn, partition_store[currentPid], K, num_dims, domains));
					}
				}
				//// context.getCounter(Counters.CanCalKNNs).increment(TrueKnnPoints.size());

				// start calculating LRD and LOF if possible

				// save those pruned points but need to recompute KNNs
				HashMap<Long, MetricObject> needCalculatePruned = new HashMap<Long, MetricObject>();
				HashMap<Long, MetricObject> lrdHM = new HashMap<Long, MetricObject>();
				// save those cannot be pruned only by LRD value...
				HashMap<Long, MetricObject> needCalLOF = new HashMap<Long, MetricObject>();
				// need more knn information, maybe knn is pruned...
				HashMap<Long, MetricObject> needRecalLRD = new HashMap<Long, MetricObject>();

				// calculate LRD for points that can calculate
				for (MetricObject mo : TrueKnnPoints.values()) {
					CalLRDForSingleObject(mo, TrueKnnPoints, CanPrunePoints, needCalculatePruned, lrdHM, needCalLOF,
							needRecalLRD, thresholdLof, context,leaveNodes);
				}

				// for those pruned by cell-based pruning, find kNNs for these
				// points
				for (MetricObject mo : needCalculatePruned.values()) {
					int tempIndex = mo.getIndexOfCPCellInList();
					prQuadLeaf curLeaf = leaveNodes.get(tempIndex).findLeafWithSmallCellIndex(
							leaveNodes.get(tempIndex).getRootForPRTree(), mo.getIndexForSmallCell()[0],
							mo.getIndexForSmallCell()[1]);
					leaveNodes.get(tempIndex).findKnnsForOnePoint(TrueKnnPoints, mo, curLeaf, leaveNodes,
							leaveNodes.get(tempIndex), ptn, partition_store[currentPid], K, num_dims, domains);

				}
				// knn's knn is pruned...
				HashMap<Long, MetricObject> needCalculateLRDPruned = new HashMap<Long, MetricObject>();
				// calculate LRD for some points again....
				for (MetricObject mo : needRecalLRD.values()) {
					ReCalLRDForSpecial(context, mo, TrueKnnPoints, needCalculatePruned, lrdHM, needCalLOF,
							needCalculateLRDPruned, thresholdLof);
				}

				// (needs implementation) calculate LRD for points that needs
				// Calculate LRD (deal with needCalculateLRDPruned)
				for (MetricObject mo : needCalculateLRDPruned.values()) {
					float lrd_core = 0.0f;
					boolean canCalLRD = true;
					for (long knn_mo : mo.getKnnInDetail().keySet()) {
						// first point out which large cell it is in
						// (tempIndexX, tempIndexY)
						float kdistknn = 0.0f;
						if (TrueKnnPoints.containsKey(knn_mo))
							kdistknn = TrueKnnPoints.get(knn_mo).getKdist();
						else if (CanPrunePoints.containsKey(knn_mo) && (!needCalculatePruned.containsKey(knn_mo))) {
							MetricObject newKnnFind = CanPrunePoints.get(knn_mo);
							int tempIndex = newKnnFind.getIndexOfCPCellInList();
							prQuadLeaf curLeaf = leaveNodes.get(tempIndex).findLeafWithSmallCellIndex(
									leaveNodes.get(tempIndex).getRootForPRTree(), newKnnFind.getIndexForSmallCell()[0],
									newKnnFind.getIndexForSmallCell()[1]);
							leaveNodes.get(tempIndex).findKnnsForOnePoint(TrueKnnPoints, newKnnFind, curLeaf,
									leaveNodes, leaveNodes.get(tempIndex), ptn, partition_store[currentPid], K,
									num_dims, domains);
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
						float temp_reach_dist = Math.max(mo.getKnnInDetail().get(knn_mo), kdistknn);
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
					// context.getCounter(Counters.PruneCalLRD).increment(needCalculateLRDPruned.size());
					// context.getCounter(Counters.UsedPrune).increment(needCalculatePruned.size());
					// context.getCounter(Counters.CanCalLRDs).increment(lrdHM.size());
					// context.getCounter(Counters.NeedLOFCal).increment(needCalLOF.size());

				// calculate LOF for points that can calculate
				for (MetricObject mo : needCalLOF.values()) {
					CalLOFForSingleObject(context, mo, lrdHM);
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
				// output data points and calculate partition bound

				for (int i = 0; i < leaveNodes.size(); i++) {
					if (leaveNodes.get(i).isSafeArea()) {
						for (MetricObject o_R : leaveNodes.get(i).getListOfPoints()) {
							if (!o_R.isCanPrune() && o_R.getType() != 'O')
								outputMultipleTypeData(o_R, outputKDistPath, lrdHM, needCalculatePruned, context);
						}
					} else {
						for (MetricObject o_R : leaveNodes.get(i).getListOfPoints()) {
							if (pointInsideSafeArea(safeArea, ((Record) o_R.getObj()).getValue())
									&& (o_R.isCanPrune() || o_R.getType() == 'O'))
								continue;
							else
								outputMultipleTypeData(o_R, outputKDistPath, lrdHM, needCalculatePruned, context);
						}
					}
				}

				// output partition plan
				mos.write(new LongWritable(1),
						new Text(currentPid + SQConfig.sepStrForRecord + partition_store[currentPid][0]
								+ SQConfig.sepStrForRecord + partition_store[currentPid][1] + SQConfig.sepStrForRecord
								+ partition_store[currentPid][2] + SQConfig.sepStrForRecord
								+ partition_store[currentPid][3] + SQConfig.sepStrForRecord + partitionExpand[0]
								+ SQConfig.sepStrForRecord + partitionExpand[1] + SQConfig.sepStrForRecord
								+ partitionExpand[2] + SQConfig.sepStrForRecord + partitionExpand[3]),
						outputPPPath + "/pp_" + context.getTaskAttemptID());
				System.err.println("computation finished");
			}
		} // end reduce function

		public boolean pointInsideSafeArea(float[] safeArea, float[] coordinates) {
			if (coordinates[0] >= safeArea[0] && coordinates[0] <= safeArea[1] && coordinates[1] >= safeArea[2]
					&& coordinates[1] <= safeArea[3])
				return true;
			else
				return false;
		}

		public boolean areaInsideSafeArea(float[] safeArea, float[] extendedArea) {
			if (extendedArea[0] >= safeArea[0] && extendedArea[1] <= safeArea[1] && extendedArea[2] >= safeArea[2]
					&& extendedArea[3] <= safeArea[3])
				return true;
			else
				return false;
		}

		/**
		 * Calculate LRD if possible
		 * 
		 * @param context
		 * @param o_S
		 * @param TrueKnnPoints
		 * @param CanPrunePoints
		 * @param needCalculatePruned
		 * @param lrdHM
		 * @param needCalLOF
		 * @param threshold
		 * @return
		 * @throws IOException
		 * @throws InterruptedException
		 */
		private boolean CalLRDForSingleObject(MetricObject o_S, HashMap<Long, MetricObject> TrueKnnPoints,
				HashMap<Long, MetricObject> CanPrunePoints, HashMap<Long, MetricObject> needCalculatePruned,
				HashMap<Long, MetricObject> lrdHM, HashMap<Long, MetricObject> needCalLOF,
				HashMap<Long, MetricObject> needRecalLRD, float threshold, Context context,
				ArrayList<LargeCellStore> leaveNodes) throws IOException, InterruptedException {
			float lrd_core = 0.0f;
			float reachdistMax = 0.0f;
			float minNNtoNN = Float.POSITIVE_INFINITY;
			// boolean canLRD = true;
			int countPruned = 0;
			HashMap<Long, MetricObject> tempNeedCalculatePruned = new HashMap<Long, MetricObject>();
			for (Map.Entry<Long, Float> entry : o_S.getKnnInDetail().entrySet()) {
				long temp_kNNKey = entry.getKey();
				float temp_dist = entry.getValue();
				if (!TrueKnnPoints.containsKey(temp_kNNKey)) {
					if (CanPrunePoints.containsKey(temp_kNNKey)) {
						tempNeedCalculatePruned.put(temp_kNNKey, CanPrunePoints.get(temp_kNNKey));
						reachdistMax = Math.max(temp_dist, reachdistMax);
						minNNtoNN = Math.min(minNNtoNN,
								leaveNodes.get(CanPrunePoints.get(temp_kNNKey).getIndexOfCPCellInList()).getCpDist());
						continue;
					} else
						return false;
				}
				float temp_reach_dist = Math.max(temp_dist, TrueKnnPoints.get(temp_kNNKey).getKdist());
				reachdistMax = Math.max(reachdistMax, temp_reach_dist);
				minNNtoNN = Math.min(minNNtoNN, TrueKnnPoints.get(temp_kNNKey).getNearestNeighborDist());
				lrd_core += temp_reach_dist;
			}
			// if (!canLRD) {
			// needRecalLRD.put(((Record) o_S.getObj()).getRId(), o_S);
			// return false;
			// }
			if (tempNeedCalculatePruned.isEmpty()) {
				lrd_core = 1.0f / (lrd_core / K * 1.0f);
				o_S.setLrdValue(lrd_core);
				o_S.setType('L');
				// calculate if this can prune? if can prune, then don't
				// calculate
				// lof
				float predictedLOF = reachdistMax / minNNtoNN;
				lrdHM.put(((Record) o_S.getObj()).getRId(), o_S);
				if (predictedLOF <= threshold) {
					o_S.setCanPrune(true);
					countPruned++;
				} else
					needCalLOF.put(((Record) o_S.getObj()).getRId(), o_S);
				// System.out.println("LRD-----" + lrd_core);
				context.getCounter(Counters.LRDPrunedPoints).increment(countPruned);
				return true;
			} else {
				if (reachdistMax / minNNtoNN <= threshold) {
					o_S.setCanPrune(true);
					countPruned++;
					context.getCounter(Counters.LRDPrunedPoints).increment(countPruned);
					return true;
				} else {
					needCalculatePruned.putAll(tempNeedCalculatePruned);
					needRecalLRD.put(((Record) o_S.getObj()).getRId(), o_S);
					return false;
				}
			}
		}

		private void CalLOFForSingleObject(Context context, MetricObject o_S, HashMap<Long, MetricObject> lrdHm)
				throws IOException, InterruptedException {
			float lof_core = 0.0f;
			int countPruned = 0;
			if (o_S.getLrdValue() == 0)
				lof_core = 0;
			else {
				for (Map.Entry<Long, Float> entry : o_S.getKnnInDetail().entrySet()) {
					long temp_kNNKey = entry.getKey();

					if (!lrdHm.containsKey(temp_kNNKey)) {
						return;
					}
					float temp_lrd = lrdHm.get(temp_kNNKey).getLrdValue();
					if (temp_lrd == 0 || o_S.getLrdValue() == 0)
						continue;
					else
						lof_core += temp_lrd / o_S.getLrdValue() * 1.0f;
				}
				lof_core = lof_core / K * 1.0f;
			}
			if (Float.isNaN(lof_core) || Float.isInfinite(lof_core))
				lof_core = 0;
			o_S.setLofValue(lof_core);
			o_S.setType('O'); // calculated LOF
			// context.getCounter(Counters.CanCalLOFs).increment(1);
			if (lof_core <= thresholdLof) {
				// context.getCounter(Counters.ThirdPrune).increment(1);
				countPruned++;
				o_S.setCanPrune(true);
			}
			context.getCounter(Counters.FinalPrunedPoints).increment(countPruned);
		}

		/**
		 * Calculate LRD for some points that knns pruned
		 * 
		 * @param context
		 * @param o_S
		 * @param TrueKnnPoints
		 * @param needCalculatePruned
		 * @param lrdHM
		 * @param needCalLOF
		 * @param threshold
		 * @return
		 * @throws IOException
		 * @throws InterruptedException
		 */
		private boolean ReCalLRDForSpecial(Context context, MetricObject o_S, HashMap<Long, MetricObject> TrueKnnPoints,
				HashMap<Long, MetricObject> needCalculatePruned, HashMap<Long, MetricObject> lrdHM,
				HashMap<Long, MetricObject> needCalLOF, HashMap<Long, MetricObject> needCalculateLRDPruned,
				float threshold) throws IOException, InterruptedException {
			float lrd_core = 0.0f;
			float reachdistMax = 0.0f;
			float minNNtoNN = Float.POSITIVE_INFINITY;
			int countPruned = 0;

			for (Map.Entry<Long, Float> entry : o_S.getKnnInDetail().entrySet()) {
				long temp_kNNKey = entry.getKey();
				float temp_dist = entry.getValue();
				float temp_reach_dist = 0.0f;
				if (!TrueKnnPoints.containsKey(temp_kNNKey)) {
					if (needCalculatePruned.containsKey(temp_kNNKey)) {
						temp_reach_dist = Math.max(temp_dist, needCalculatePruned.get(temp_kNNKey).getKdist());
						reachdistMax = Math.max(reachdistMax, temp_reach_dist);
						minNNtoNN = Math.min(minNNtoNN, needCalculatePruned.get(temp_kNNKey).getNearestNeighborDist());
					}
				} else {
					temp_reach_dist = Math.max(temp_dist, TrueKnnPoints.get(temp_kNNKey).getKdist());
					reachdistMax = Math.max(reachdistMax, temp_reach_dist);
					minNNtoNN = Math.min(minNNtoNN, TrueKnnPoints.get(temp_kNNKey).getNearestNeighborDist());
				}
				lrd_core += temp_reach_dist;
			}
			lrd_core = 1.0f / (lrd_core / K * 1.0f);
			o_S.setLrdValue(lrd_core);
			o_S.setType('L');
			// calculate if this can prune? if can prune, then don't calculate
			// lof
			float predictedLOF = reachdistMax / minNNtoNN;
			lrdHM.put(((Record) o_S.getObj()).getRId(), o_S);
			if (predictedLOF <= threshold) {
				// context.getCounter(Counters.SecondPrune).increment(1);
				o_S.setCanPrune(true);
				countPruned++;
			} else {
				needCalLOF.put(((Record) o_S.getObj()).getRId(), o_S);
				for (Map.Entry<Long, Float> entry : o_S.getKnnInDetail().entrySet()) {
					long temp_kNNKey = entry.getKey();
					if (needCalculatePruned.containsKey(temp_kNNKey))
						needCalculateLRDPruned.put(((Record) needCalculatePruned.get(temp_kNNKey).getObj()).getRId(),
								needCalculatePruned.get(temp_kNNKey));
				}
			}
			// System.out.println("LRD-----" + lrd_core);
			context.getCounter(Counters.LRDPrunedPoints).increment(countPruned);
			return true;
		}

		/**
		 * output different types of data points in multiple files
		 * 
		 * @param context
		 * @param o_R
		 * @throws InterruptedException
		 * @throws IOException
		 */
		public void outputMultipleTypeData(MetricObject o_R, String outputKDistPath, HashMap<Long, MetricObject> lrdHM,
				HashMap<Long, MetricObject> needCalculatePruned, Context context)
				throws IOException, InterruptedException {
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
				for (Map.Entry<Long, Float> entry : o_R.getKnnInDetail().entrySet()) {
					long keyMap = entry.getKey();
					float valueMap = entry.getValue();
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

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "Random selection: First KNN Find");

		job.setJarByClass(LargeCellBasedKnnFind.class);
		job.setMapperClass(CellBasedKNNFinderMapper.class);

		/** set multiple output path */
		MultipleOutputs.addNamedOutput(job, "partitionplan", TextOutputFormat.class, LongWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "kdistance", TextOutputFormat.class, LongWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "topnLOF", TextOutputFormat.class, LongWritable.class, Text.class);

		job.setReducerClass(CellBasedKNNFinderReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));

		String strFSName = conf.get("fs.default.name");
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.dataset)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strKnnSummaryOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strKnnSummaryOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strPartitionPlanOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strCellsOutput)));

		/** print job parameter */
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, 10));
		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) {
		LargeCellBasedKnnFind findKnnAndSupporting = new LargeCellBasedKnnFind();
		try {
			findKnnAndSupporting.run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
