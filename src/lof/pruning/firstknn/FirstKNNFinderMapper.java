package lof.pruning.firstknn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

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

import metricspace.MetricKey;
import sampling.CellStore;
import util.SQConfig;

public class FirstKNNFinderMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	/**
	 * The dimension of data (set by user, now only support dimension of 2, if
	 * change to 3 or more, has to change some codes)
	 */
	private static int num_dims = 2;
	/**
	 * number of small cells per dimension: when come with a node, map to a
	 * range (divide the domain into small_cell_num_per_dim) (set by user)
	 */
	private static int cell_num = 501;

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
	private static double[] partition_priority;
	/** save each small buckets. in order to speed up mapping process */
	private static CellStore[] cell_store;
	/**
	 * Number of desired partitions in each dimension (set by user), for Data
	 * Driven partition
	 */
	// private static int di_numBuckets;
	private static int num_partitions;
	private static ArrayList<Integer>[] partitionAdj;

	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		/** get configuration from file */
		num_dims = conf.getInt(SQConfig.strDimExpression, 2);
		cell_num = conf.getInt(SQConfig.strNumOfSmallCells, 501);
		domains = new float[2];
		domains[0] = conf.getFloat(SQConfig.strDomainMin, 0.0f);
		domains[1] = conf.getFloat(SQConfig.strDomainMax, 10001.0f);
		smallRange = (int) Math.ceil((domains[1] - domains[0]) / cell_num);

		// di_numBuckets = conf.getInt(SQConfig.strNumOfPartitions, 2);

		/** parse files in the cache */
		try {
			URI[] cacheFiles = context.getCacheArchives();

			if (cacheFiles == null || cacheFiles.length < 4) {
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
						partition_store = new float[num_partitions][num_dims * 2];
						partitionAdj = new ArrayList[num_partitions];
						for (int ii = 0; ii < num_partitions; ii++) {
							partitionAdj[ii] = new ArrayList<Integer>();
						}
						partition_priority = new double[num_partitions];
						currentReader.close();
						currentStream.close();
					}
				}
			} // end for URI

			// then read in others
			for (URI path : cacheFiles) {
				String filename = path.toString();
				FileSystem fs = FileSystem.get(conf);

				FileStatus[] stats = fs.listStatus(new Path(filename));
				for (int i = 0; i < stats.length; ++i) {
					if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("pp-r")) {
						System.out.println("Reading partition plan from " + stats[i].getPath().toString());
						FSDataInputStream currentStream;
						BufferedReader currentReader;
						currentStream = fs.open(stats[i].getPath());
						currentReader = new BufferedReader(new InputStreamReader(currentStream));
						String line;
						while ((line = currentReader.readLine()) != null) {
							/** parse line */

							String[] splitsStr = line.split(SQConfig.sepStrForRecord);
							int tempid = Integer.parseInt(splitsStr[0]);
							for (int j = 0; j < num_independentDim; j++) {
								partition_store[tempid][2 * indexOfIndependentDims[j]] = Float
										.parseFloat(splitsStr[2 * j + 1]);
								partition_store[tempid][2 * indexOfIndependentDims[j] + 1] = Float
										.parseFloat(splitsStr[2 * j + 2]);
							}
							for (int j = 0; j < num_correlatedDim; j++) {
								// partition_store[tempid][2 *
								// indexOfCorrelatedDims[j]] = domains[0];
								// partition_store[tempid][2 *
								// indexOfCorrelatedDims[j] + 1] =
								// domains[1];
								partition_store[tempid][2 * indexOfCorrelatedDims[j]] = Float.POSITIVE_INFINITY;
								partition_store[tempid][2 * indexOfCorrelatedDims[j] + 1] = Float.NEGATIVE_INFINITY;
							}
							partition_priority[tempid] = Double.parseDouble(splitsStr[num_independentDim * 2 + 1]);
						}
						currentReader.close();
						currentStream.close();
					} else if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("part-r")) {
						System.out.println("Reading cells for partitions from " + stats[i].getPath().toString());
						FSDataInputStream currentStream;
						BufferedReader currentReader;
						currentStream = fs.open(stats[i].getPath());
						currentReader = new BufferedReader(new InputStreamReader(currentStream));
						String line;
						while ((line = currentReader.readLine()) != null) {
							/** parse line */
							String[] items = line.split(SQConfig.sepStrForRecord);
							if (items.length == 2) {
								int cellId = Integer.valueOf(items[0]);
								int corePartitionId = Integer.valueOf(items[1].substring(2));
								cell_store[cellId] = new CellStore(cellId, corePartitionId);
							}
						} // end while
						currentReader.close();
						currentStream.close();
					} else if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("adjacency.csv")) {
						System.out.println("Reading partition adjacency from " + stats[i].getPath().toString());
						FSDataInputStream currentStream;
						BufferedReader currentReader;
						currentStream = fs.open(stats[i].getPath());
						currentReader = new BufferedReader(new InputStreamReader(currentStream));
						String line;
						while ((line = currentReader.readLine()) != null) {
							/** parse line */
							String[] strSplits = line.split(":");
							int tempId = Integer.parseInt(strSplits[0]);
							if (strSplits[1].length() != 0) {
								String[] tempValues = strSplits[1].split(SQConfig.sepStrForRecord);
								for (String str : tempValues) {
									partitionAdj[tempId].add(Integer.parseInt(str));
								}
							}
						} // end while
						currentReader.close();
						currentStream.close();
					}
				} // end for (int i = 0; i < stats.length; ++i)
			} // end for (URI path : cacheFiles)

		} catch (IOException ioe) {
			System.err.println("Caught exception while getting cached files");
		}
		// end parse file
		// init correlated domain to [+inf -inf]

		// for (int i = 0; i < num_partitions; i++) {
		// for (int j = 0; j < num_correlatedDim; j++) {
		// partition_store[i][indexOfCorrelatedDims[j] * 2] =
		// Float.POSITIVE_INFINITY;
		// partition_store[i][indexOfCorrelatedDims[j] * 2 + 1] =
		// Float.NEGATIVE_INFINITY;
		// }
		// }
		int tempid = 0;
		// System.out.println(tempid + "," + partition_store[tempid][0] +
		// "," + partition_store[tempid][1] + ","
		// + partition_store[tempid][2] + "," + partition_store[tempid][3] +
		// "," + partition_store[tempid][4]
		// + "," + partition_store[tempid][5] + "," +
		// partition_store[tempid][6] + ","
		// + partition_store[tempid][7]);
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Variables
		float[] crds = new float[num_independentDim]; // coordinates of one
														// input
		// data
		String[] splitStr = value.toString().split(SQConfig.sepStrForRecord);
		// parse raw input data into coordinates/crds
		for (int i = 0; i < num_independentDim; i++) {
			// System.out.println(value.toString());
			crds[i] = Float.parseFloat(splitStr[indexOfIndependentDims[i] + 1]);
		}
		// find which cell the point in
		int cell_id = CellStore.ComputeCellStoreId(crds, num_independentDim, cell_num, smallRange);
		if (cell_id < 0)
			return;
		// partition id = core area partition id
		int blk_id = cell_store[cell_id].core_partition_id;
		if (blk_id < 0)
			return;
		// context.write(new MetricKey(blk_id, partition_priority[blk_id]),
		// value);
		context.write(new IntWritable(blk_id), value);
		for (int i = 0; i < num_correlatedDim; i++) {
			float tempNum = Float.parseFloat(splitStr[indexOfCorrelatedDims[i] + 1]);
			if (tempNum < partition_store[blk_id][indexOfCorrelatedDims[i] * 2])
				partition_store[blk_id][indexOfCorrelatedDims[i] * 2] = tempNum;
			if (tempNum > partition_store[blk_id][indexOfCorrelatedDims[i] * 2 + 1])
				partition_store[blk_id][indexOfCorrelatedDims[i] * 2 + 1] = tempNum;
		}
		// context.write(new MetricKey(blk_id, 1), value);
		// context.write(new IntWritable(blk_id), value);
	}

	public String wrapUpPartitionInformationToString(int i) {
		String outputStr = "PInfo:" + i + ",";
		for (int j = 0; j < partition_store[i].length; j++) {
			outputStr += partition_store[i][j] + ",";
		}
		outputStr = outputStr.substring(0, outputStr.length() - 1);
		return outputStr;
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		// output partition information for each adjacent partition
		for (int i = 0; i < num_partitions; i++) {
			// context.write(new MetricKey(i, Float.POSITIVE_INFINITY),
			// new Text(wrapUpPartitionInformationToString(i)));
			context.write(new IntWritable(i), new Text(wrapUpPartitionInformationToString(i)));
			for (Integer adjPartition : partitionAdj[i]) {
				// context.write(new MetricKey(i, Float.POSITIVE_INFINITY),
				// new Text(wrapUpPartitionInformationToString(adjPartition)));
				context.write(new IntWritable(i), new Text(wrapUpPartitionInformationToString(adjPartition)));
			}
		}
	}
}
