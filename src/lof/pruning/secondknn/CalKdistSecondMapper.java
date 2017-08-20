package lof.pruning.secondknn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import sampling.CellStore;
import util.SQConfig;

public class CalKdistSecondMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	/**
	 * The dimension of data (set by user, now only support dimension of 2, if
	 * change to 3 or more, has to change some codes)
	 */
	private static int num_dims = 2;
	/**
	 * number of small cells per dimension: when come with a node, map to a
	 * range (divide the domain into small_cell_num_per_dim) (set by user)
	 */
	public static int cell_num = 501;

	/** The domains. (set by user) */
	private static float[] domains;
	/** size of each small buckets */
	private static int smallRange;

	private int[] indexOfIndependentDims;
	private int num_independentDim;
	private int num_correlatedDim;
	private int[] indexOfCorrelatedDims;

	/**
	 * block list, which saves each block's info including start & end positions
	 * on each dimension. print for speed up "mapping"
	 */
	private static float[][] partition_store;
	/** save each small buckets. in order to speed up mapping process */
	private static CellStore[] cell_store;
	/**
	 * Number of desired partitions in each dimension (set by user), for Data
	 * Driven partition
	 */
	// private static int di_numBuckets;

	private static int K;

	// private static float thresholdLof = 0.0f;

	private static int maxLimitSupportingArea;

	private static int num_partitions;

	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		/** get configuration from file */
		num_dims = conf.getInt(SQConfig.strDimExpression, 2);
		cell_num = conf.getInt(SQConfig.strNumOfSmallCells, 501);
		domains = new float[2];
		domains[0] = conf.getFloat(SQConfig.strDomainMin, 0.0f);
		domains[1] = conf.getFloat(SQConfig.strDomainMax, 10001.0f);
		smallRange = (int) Math.ceil((domains[1] - domains[0]) / cell_num);
		K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
		maxLimitSupportingArea = conf.getInt(SQConfig.strMaxLimitSupportingArea, 5000);

		/** parse files in the cache */
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
						cell_store = new CellStore[(int) Math.pow(cell_num, num_independentDim)];

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
					// else if (!stats[i].isDirectory()
					// &&
					// stats[i].getPath().toString().contains(conf.get(SQConfig.strTopNFirstSummary))
					// &&
					// stats[i].getPath().toString().contains("part-r-00000")) {
					// System.out.println("Reading threshold from " +
					// stats[i].getPath().toString());
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
					else if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("part-r")) {
						System.out.println("Reading cells for partitions from " + stats[i].getPath().toString());
						FSDataInputStream currentStream;
						BufferedReader currentReader;
						currentStream = fs.open(stats[i].getPath());
						currentReader = new BufferedReader(new InputStreamReader(currentStream));
						String line;
						while ((line = currentReader.readLine()) != null) {
							/** parse line */
							String[] items = line.split(SQConfig.sepStrForRecord);
							if (items.length == 3) {
								int cellId = Integer.parseInt(items[0]);
								int corePartitionId = Integer.valueOf(items[1].substring(2));
								cell_store[cellId] = new CellStore(cellId, corePartitionId);
								if (items[2].length() > 1) { // has support
																// cells
									String[] splitStr = items[2].substring(2, items[2].length())
											.split(SQConfig.sepSplitForIDDist);
									for (int j = 0; j < splitStr.length; j++) {
										cell_store[cellId].support_partition_id.add(Integer.valueOf(splitStr[j]));
									}
								}
								// System.out.println(cell_store[cellId].printCellStoreWithSupport());
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
			if (inputStrSplits.length < 4 + num_dims) {
				return;
			}
			long pointId = Long.valueOf(inputStr.split(SQConfig.sepStrForKeyValue)[0]);
			float[] crds = new float[num_independentDim]; // coordinates of one
			// input
			// data
			// parse raw input data into coordinates/crds
			for (int i = 0; i < num_independentDim; i++) {
				crds[i] = Float.parseFloat(inputStrSplits[indexOfIndependentDims[i]]);
			}
			// core partition id
			int corePartitionId = Integer.valueOf(inputStrSplits[num_dims]);
			// find which cell the point in
			int cell_id = CellStore.ComputeCellStoreId(crds, num_independentDim, cell_num, smallRange);
			if (cell_id < 0 || cell_store[cell_id].core_partition_id != corePartitionId) {
				System.out.println("CellId does not match");
				return;
			}

			// tag
			char curTag = inputStrSplits[num_dims + 2].charAt(0);
			float curKdist = -1;
			float curLrd = -1;
			float curLof = -1;
			String curKnns = "";
			// if can prune
			boolean canPrune = false;

			if (inputStrSplits[num_dims + 1].equals("T")) {
				canPrune = true;
				// k-distance saved
				curKdist = Float.parseFloat(inputStrSplits[num_dims + 3]);
				// lrd saved
				// if (inputStrSplits.length > 6)
				curLrd = Float.parseFloat(inputStrSplits[num_dims + 4]);
				// lof saved
				// if (inputStrSplits.length > 7)
				curLof = Float.parseFloat(inputStrSplits[num_dims + 5]);
			} else { // cannot prune
						// knns
				curKnns = "";
				for (int i = num_dims + 3; i < num_dims + 3 + K; i++) {
					// if
					// (inputStrSplits[i].split(SQConfig.sepSplitForIDDist).length
					// > 1) {
					curKnns = curKnns + inputStrSplits[i] + SQConfig.sepStrForRecord;
					// }
				}
				if (curKnns.length() > 0)
					curKnns = curKnns.substring(0, curKnns.length() - 1);
				// k-distance saved
				curKdist = Float.parseFloat(inputStrSplits[num_dims + 3 + K]);
				// lrd saved
				curLrd = Float.parseFloat(inputStrSplits[num_dims + 4 + K]);
				// lof saved
				curLof = Float.parseFloat(inputStrSplits[num_dims + 5 + K]);
			}

			// build up partitions to check and save to a hash set (by core
			// area
			// and support area of the cell)
			Set<Integer> partitionToCheck = new HashSet<Integer>();
			for (Iterator itr = cell_store[cell_id].support_partition_id.iterator(); itr.hasNext();) {
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
				for (int i = 0; i < num_independentDim; i++) {
					if (crds[i] < partition_store[blk_id][2 * i + 1]
							+ partition_store[blk_id][2 * num_independentDim + 2 * i + 1]
							&& crds[i] >= partition_store[blk_id][2 * i]
									- partition_store[blk_id][2 * num_independentDim + 2 * i]) {
						belong = 2;
					} else {
						belong = 0;
						break;
					}
				} // end for(int i=0;i<numDims;i++)
					// output block key and data value

				// output block key and data value
				if (belong == 2) { // support area data
					// output to support area with a tag 'S'
					String str = pointId + SQConfig.sepStrForRecord;
					for (int i = 0; i < num_dims; i++) {
						str += inputStrSplits[i] + SQConfig.sepStrForRecord;
					}
					context.write(new IntWritable(blk_id), new Text(str + 'S' + SQConfig.sepStrForRecord + curTag
							+ SQConfig.sepStrForRecord + curKdist + SQConfig.sepStrForRecord + curLrd));
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
				String str = pointId + SQConfig.sepStrForRecord;
				for (int i = 0; i < num_dims; i++) {
					str += inputStrSplits[i] + SQConfig.sepStrForRecord;
				}
				context.write(new IntWritable(corePartitionId),
						new Text(str + 'C' + SQConfig.sepStrForRecord + curTag + SQConfig.sepStrForRecord + curKnns
								+ SQConfig.sepStrForRecord + curKdist + SQConfig.sepStrForRecord + curLrd
								+ SQConfig.sepStrForRecord + whoseSupport));
			}
		} catch (Exception e) {
			System.err.println("Input String: " + inputStr);
		}
	} // end map function
} // end map class