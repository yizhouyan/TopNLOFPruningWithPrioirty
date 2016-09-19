package lof.pruning;

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
	 * @version Dec 31, 2015
	 */

	static enum Counters {
		// PhaseTime, OutputTime, CannotFindKPointsAsNeighbors
		CountDuplicatePoints,
	}

	public static class CalKdistSecondMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
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

		private static int K;

		private static float thresholdLof = 0.0f;

		private static int maxLimitSupportingArea;
		
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
			partition_store = new float[di_numBuckets[0] * di_numBuckets[1]][num_dims * 2 + 4];
			K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
			maxLimitSupportingArea = conf.getInt(SQConfig.strMaxLimitSupportingArea, 5000);
			
			/** parse files in the cache */
			try {
				URI[] cacheFiles = context.getCacheArchives();

				if (cacheFiles == null || cacheFiles.length < 3) {
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
								try {
									String[] splitsStr = line.split(SQConfig.sepStrForKeyValue)[1]
											.split(SQConfig.sepStrForRecord);
									int tempid = Integer.parseInt(splitsStr[0]);
									for (int j = 0; j < num_dims * 2 + 4; j++) {
										partition_store[tempid][j] = Float.parseFloat(splitsStr[j + 1]);
									}
									if (partition_store[tempid][4] >= maxLimitSupportingArea)
										partition_store[tempid][4] = maxLimitSupportingArea;
									if (partition_store[tempid][5] >= maxLimitSupportingArea)
										partition_store[tempid][5] = maxLimitSupportingArea;
									if (partition_store[tempid][6] >= maxLimitSupportingArea)
										partition_store[tempid][6] = maxLimitSupportingArea;
									if (partition_store[tempid][7] >= maxLimitSupportingArea)
										partition_store[tempid][7] = maxLimitSupportingArea;
								} catch (Exception e) {
									System.err.println("Line: " + line);
								}
							}
							currentReader.close();
							currentStream.close();
						} else if (!stats[i].isDirectory()
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
								if (items.length >= 6) {
									int x_1 = (int) (Float.valueOf(items[0]) / smallRange);
									int y_1 = (int) (Float.valueOf(items[2]) / smallRange);
									cell_store[x_1][y_1] = new CellStore(x_1 * smallRange, (x_1 + 1) * smallRange,
											(y_1) * smallRange, (y_1 + 1) * smallRange);
									cell_store[x_1][y_1].core_partition_id = Integer.valueOf(items[4].substring(2));

									for (int j = 5; j < items.length; j++) {
										if (j == 5 && (!items[j].contains(":"))) {
											break;
										}
										if (j == 5) {
											cell_store[x_1][y_1].support_partition_id
													.add(Integer.valueOf(items[5].substring(2)));
										} else {
											cell_store[x_1][y_1].support_partition_id.add(Integer.valueOf(items[j]));
										}
									}
									// System.out.println(cell_store[x_1][y_1].printCellStoreWithSupport());
								}
							}
							currentReader.close();
							currentStream.close();
						} // end else if
					} // end for (int i = 0; i < stats.length; ++i)
				} // end for (URI path : cacheFiles)

			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files");
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Variables
			// output format key:nid value: point value, partition
			// id,canPrune,tag,
			// (KNN's nid and dist and kdist and lrd),k-distance, lrd, lof,
			String inputStr = value.toString();
			 try {
			String[] inputStrSplits = inputStr.split(SQConfig.sepStrForKeyValue)[1].split(SQConfig.sepStrForRecord);
			long pointId = Long.valueOf(inputStr.split(SQConfig.sepStrForKeyValue)[0]);
			float[] crds = new float[num_dims]; // coordinates of one
												// input
												// data
			// parse raw input data into coordinates/crds
			for (int i = 0; i < num_dims; i++) {
				crds[i] = Float.parseFloat(inputStrSplits[i]);
			}
			// core partition id
			int corePartitionId = Integer.valueOf(inputStrSplits[2]);
			// find which cell the point in
			int x_cellstore = (int) (Math.floor(crds[0] / smallRange));
			int y_cellstore = (int) (Math.floor(crds[1] / smallRange));

			// tag
			char curTag = inputStrSplits[4].charAt(0);
			float curKdist = -1;
			float curLrd = -1;
			float curLof = -1;
			String curKnns = "";
			// if can prune
			boolean canPrune = false;
			if (inputStrSplits[3].equals("T")) {
				canPrune = true;
				// k-distance saved
				curKdist = Float.parseFloat(inputStrSplits[5]);
				// lrd saved
				// if (inputStrSplits.length > 6)
				curLrd = Float.parseFloat(inputStrSplits[6]);
				// lof saved
				// if (inputStrSplits.length > 7)
				curLof = Float.parseFloat(inputStrSplits[7]);
			} else { // cannot prune
						// knns
				curKnns = "";
				for (int i = 5; i < 5 + K; i++) {
					// if
					// (inputStrSplits[i].split(SQConfig.sepSplitForIDDist).length
					// > 1) {
					curKnns = curKnns + inputStrSplits[i] + SQConfig.sepStrForRecord;
					// }
				}
				if (curKnns.length() > 0)
					curKnns = curKnns.substring(0, curKnns.length() - 1);
				// k-distance saved
				curKdist = Float.parseFloat(inputStrSplits[5 + K]);
				// lrd saved
				curLrd = Float.parseFloat(inputStrSplits[6 + K]);
				// lof saved
				curLof = Float.parseFloat(inputStrSplits[7 + K]);
			}

			// build up partitions to check and save to a hash set (by core
			// area
			// and support area of the cell)
			Set<Integer> partitionToCheck = new HashSet<Integer>();
			partitionToCheck.add(cell_store[x_cellstore][y_cellstore].core_partition_id);

			for (Iterator itr = cell_store[x_cellstore][y_cellstore].support_partition_id.iterator(); itr.hasNext();) {
				int keyiter = (Integer) itr.next();
				partitionToCheck.add(keyiter);
			}

			String whoseSupport = "";
			// traverse each block to find belonged regular or extended
			// block
			for (Iterator iter = partitionToCheck.iterator(); iter.hasNext();) {
				int blk_id = (Integer) iter.next();
				if (blk_id < 0) {
					// System.out.println("Block id: " + blk_id);
					continue;
				}
				// for(int blk_id=0;blk_id<markEndPoints(crds[0]);blk_id++)
				// {
				int belong = 0; // indicate whether the point belongs, 0 ->
								// neither; 1 -> regular; 2-> extend
				// traverse block's start & end positions in each dimension
				for (int i = 0; i < num_dims; i++) {
					// check if the point belongs to current regular block
					// or
					// this block's extended area
					if (crds[i] < partition_store[blk_id][2 * i + 1] + partition_store[blk_id][2 * i + 1 + 4]
							&& crds[i] >= partition_store[blk_id][2 * i] - partition_store[blk_id][2 * i + 4]) {
						// check if the point belongs to current regular
						// block
						if (crds[i] >= partition_store[blk_id][2 * i] && crds[i] < partition_store[blk_id][2 * i + 1]) {
							if (belong != 2) {
								belong = 1;
							}
						}
						// otherwise the point belongs to this block's
						// extended
						// area
						else {
							belong = 2;
						}
					} else {
						belong = 0;
						break;
					}
				} // end for(int i=0;i<numDims;i++)

				// output block key and data value
				if (belong == 2) { // support area data
					// output to support area with a tag 'S'
					context.write(new IntWritable(blk_id),
							new Text(pointId + SQConfig.sepStrForRecord + crds[0] + SQConfig.sepStrForRecord + crds[1]
									+ SQConfig.sepStrForRecord + 'S' + SQConfig.sepStrForRecord + curTag
									+ SQConfig.sepStrForRecord + curKdist + SQConfig.sepStrForRecord + curLrd
									+ SQConfig.sepStrForRecord + curLof));
					// context.getCounter(Counters.DuplicationRate).increment(1);
					// save information to whoseSupport
					whoseSupport = whoseSupport + blk_id + SQConfig.sepStrForIDDist;

				} // end if
			} // end for(int blk_id=0;blk_id<blocklist.length;blk_id++)
			if (whoseSupport.length() > 0)
				whoseSupport = whoseSupport.substring(0, whoseSupport.length() - 1);

			// output core area (For those not be pruned)
			// format key : core partition id value: nid,node
			// information,canPrune,tag,
			// (KNN's nid and dist),k-distance, lrd, lof,
			if ((!canPrune) && (curTag != 'O')) {
				context.write(new IntWritable(corePartitionId),
						new Text(pointId + SQConfig.sepStrForRecord + crds[0] + SQConfig.sepStrForRecord + crds[1]
								+ SQConfig.sepStrForRecord + 'C' + SQConfig.sepStrForRecord + curTag
								+ SQConfig.sepStrForRecord + curKnns + SQConfig.sepStrForRecord + curKdist
								+ SQConfig.sepStrForRecord + curLrd + SQConfig.sepStrForRecord + curLof
								+ SQConfig.sepStrForRecord + whoseSupport));
			}
			 }
			 catch (Exception e) {
			 System.err.println("Input String: " + inputStr);
			 }
		} // end map function
	} // end map class

	public static class CalKdistSecondReducer extends Reducer<IntWritable, Text, LongWritable, Text> {
		/**
		 * The dimension of data (set by user, now only support dimension of 2,
		 * if change to 3 or more, has to change some codes)
		 */
		private static int num_dims = 2;
		private static int K;
		private IMetricSpace metricSpace = null;
		private IMetric metric = null;
		private static float[][] partition_store;
		/** The domains. (set by user) */
		private static float[][] domains;

		/**
		 * Number of desired partitions in each dimension (set by user), for
		 * Data Driven partition
		 */
		private static int[] di_numBuckets;

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
			domains = new float[num_dims][2];
			domains[0][0] = domains[1][0] = conf.getFloat(SQConfig.strDomainMin, 0.0f);
			domains[0][1] = domains[1][1] = conf.getFloat(SQConfig.strDomainMax, 10001f);
			num_dims = conf.getInt(SQConfig.strDimExpression, 2);
			K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
			di_numBuckets = new int[num_dims];
			for (int i = 0; i < num_dims; i++) {
				di_numBuckets[i] = conf.getInt(SQConfig.strNumOfPartitions, 2);
			}
			partition_store = new float[di_numBuckets[0] * di_numBuckets[1]][num_dims * 2 + 4];
			// topNNumber = conf.getInt(SQConfig.strLOFThreshold, 100);
			try {
				URI[] cacheFiles = context.getCacheArchives();

				if (cacheFiles == null || cacheFiles.length < 3) {
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
								try {
									String[] splitsStr = line.split(SQConfig.sepStrForKeyValue)[1]
											.split(SQConfig.sepStrForRecord);
									int tempid = Integer.parseInt(splitsStr[0]);
									for (int j = 0; j < num_dims * 2 + 4; j++) {
										partition_store[tempid][j] = Float.parseFloat(splitsStr[j + 1]);
									}
								} catch (Exception e) {
									System.err.println("Line: " + line);
								}
							}
							currentReader.close();
							currentStream.close();
						}
						// else if (!stats[i].isDirectory()
						// &&
						// stats[i].getPath().toString().contains(conf.get(SQConfig.strTopNFirstSummary))
						// &&
						// stats[i].getPath().toString().contains("part-r-00000"))
						// {
						// // System.out.println("Reading threshold from " +
						// // stats[i].getPath().toString());
						// FSDataInputStream currentStream;
						// BufferedReader currentReader;
						// currentStream = fs.open(stats[i].getPath());
						// currentReader = new BufferedReader(new
						// InputStreamReader(currentStream));
						// String line = currentReader.readLine();
						// thresholdLof =
						// Float.parseFloat(line.split(SQConfig.sepStrForKeyValue)[1]);
						// System.out.println("The threshold for LOF is " +
						// thresholdLof);
						// }
					} // end for (int i = 0; i < stats.length; ++i)
				} // end for (URI path : cacheFiles)

			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files");
			}
		}

		/**
		 * parse objects in supporting area key: partition id value: point id,
		 * point information(2-d), tag(S)， tag(FTLO), kdist, lrd, lof
		 * 
		 * @param key:
		 *            partition id
		 * @param strInput:
		 *            point id, point information(2-d), tag(S),tag(FTLO), kdist,
		 *            lrd, lof
		 * @return
		 */
		private MetricObjectMore parseSupportObject(int key, String strInput) {
			int partition_id = key;
			int offset = 0;
			Object obj = metricSpace.readObject(strInput.substring(offset, strInput.length() - 2), num_dims);
			String[] tempSubString = strInput.split(SQConfig.sepStrForRecord);
			char curTag = tempSubString[num_dims + 1].charAt(0);
			if (curTag != 'S')
				System.out.println("Error Support area: " + strInput);
			char orgTag = tempSubString[num_dims + 2].charAt(0);
			float kdist = Float.parseFloat(tempSubString[num_dims + 3]);
			float curlrd = -1;
			float curlof = -1;
			// if (tempSubString.length > num_dims + 4)
			curlrd = Float.parseFloat(tempSubString[num_dims + 4]);
			// if (tempSubString.length > num_dims + 5)
			curlof = Float.parseFloat(tempSubString[num_dims + 5]);
			return new MetricObjectMore(partition_id, obj, curTag, orgTag, kdist, curlrd, curlof);
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

			String[] splitStrInput = strInput.split(SQConfig.sepStrForRecord);
			int partition_id = key;
			int offset = 0;
			Object obj = metricSpace.readObject(
					strInput.substring(offset,
							splitStrInput[0].length() + splitStrInput[1].length() + splitStrInput[2].length() + 2),
					num_dims);
			char curTag = splitStrInput[3].charAt(0);
			if (curTag != 'C')
				System.out.println("Error Core area: " + strInput);
			char orgTag = splitStrInput[4].charAt(0);
			// knns and kdist, lrd

			Map<Long, coreInfoKNNs> knnInDetail = new HashMap<Long, coreInfoKNNs>();
			int countKnns = 0;
			for (int i = 0; i < K; i++) {
				String[] tempSplit = splitStrInput[5 + i].split(SQConfig.sepSplitForIDDist);
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
			float curKdist = Float.parseFloat(splitStrInput[5 + countKnns]);
			float curLrd = Float.parseFloat(splitStrInput[6 + countKnns]);
			float curLof = Float.parseFloat(splitStrInput[7 + countKnns]);
			String whoseSupport = "";
			if (splitStrInput.length > 8 + countKnns)
				whoseSupport = splitStrInput[8 + countKnns];
			return new MetricObjectMore(partition_id, obj, curTag, orgTag, knnInDetail, curKdist, curLrd, curLof,
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
			// build up a large cell store so that can build a quadtree
			LargeCellStoreMore lcs = new LargeCellStoreMore(partition_store[key.get()][0] - partition_store[key.get()][4],
					partition_store[key.get()][1] + partition_store[key.get()][5],
					partition_store[key.get()][2] - partition_store[key.get()][6],
					partition_store[key.get()][3] + partition_store[key.get()][7], metric, metricSpace);
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
			float[] safeArea = { 0.0f, 0.0f, 0.0f, 0.0f };
			lcs.seperateLargeNoPrune(K);

			// // search knn for core points
			if (lcs.isBreakIntoSmallCells()) {
				for (MetricObjectMore mo : corePoints.values()) {
					if (mo.getOrgType() == 'F') {
						prQuadLeafMore curLeaf = lcs.findLeafWithSmallCellIndex(lcs.getRootForPRTree(),
								mo.getIndexForSmallCell()[0], mo.getIndexForSmallCell()[1]);
						// then find kNNs for this point (through the tree)
						lcs.findKnnsForOnePointSecondTime(mo, curLeaf, lcs, supportingPoints,
								partition_store[key.get()], K, num_dims, domains, context);
					}
				} // end for
			} else if (corePoints.size() != 0) {
				for (MetricObjectMore mo : corePoints.values()) {
					if (mo.getOrgType() == 'F') {
						lcs.findKnnsForOnePointInLargeCellSecondTime(mo, supportingPoints, partition_store[key.get()],
								K, num_dims, domains, context);
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
				line = line + o_S.getPartition_id() + SQConfig.sepStrForRecord + o_S.getOrgType()
						+ SQConfig.sepStrForRecord + o_S.getKdist() + SQConfig.sepStrForRecord + o_S.getLrdValue()
						+ SQConfig.sepStrForRecord + o_S.getLofValue() + SQConfig.sepStrForRecord
						+ o_S.getWhoseSupport() + SQConfig.sepStrForRecord;
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

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "Update KNNs");

		job.setJarByClass(CalKdistanceSecond.class);
		job.setMapperClass(CalKdistSecondMapper.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(CalKdistSecondReducer.class);
		job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
		// job.setNumReduceTasks(0);

		String strFSName = conf.get("fs.default.name");
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.strKdistanceOutput)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strKdistFinalOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strKdistFinalOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strKnnPartitionPlan)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strKnnCellsOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strTopNFirstSummary)));

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
