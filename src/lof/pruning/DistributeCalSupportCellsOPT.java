package lof.pruning;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import sampling.CellStore;
import util.SQConfig;

public class DistributeCalSupportCellsOPT {

	public static class DistributedSupportCellMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		public static int count = 1;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// System.out.println(value.toString());
			context.write(new IntWritable(count), new Text(value.toString()));
		}
	}

	public static class DistributedSupportCellReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		/**
		 * number of maps: when come with a node, map to a range (divide the
		 * domain into map_num) (set by user)
		 */
		private static int cell_num = 501;

		/** The domains. (set by user) */
		private static float[] domains;

		/** size of each small buckets */
		private static int smallRange;

		private static int num_independent_dims = 2;

		/**
		 * Number of desired partitions in each dimension (set by user), for
		 * Data Driven partition
		 */
		private static int di_numBuckets; ////////////////////////////////////

		/**
		 * block list, which saves each block's info including start & end
		 * positions on each dimension. print for speed up "mapping"
		 */
		private static float[][] partition_store;

		/** save each small buckets. in order to speed up mapping process */
		private static CellStore[] cell_store;

		// private static double maxOverlaps = 0;
		private static int maxLimitSupporting;

		private static int countExceedPartitions = 0;

		/**
		 * format of each line: key value(id,partition_plan,extand area)
		 * 
		 * @param fs
		 * @param filename
		 */
		public void parseFile(FileSystem fs, String filename) {
			try {
				// check if the file exists
				Path path = new Path(filename);
				// System.out.println("filename = " + filename);
				if (fs.exists(path)) {
					FSDataInputStream currentStream;
					BufferedReader currentReader;
					currentStream = fs.open(path);
					currentReader = new BufferedReader(new InputStreamReader(currentStream));
					String line;
					while ((line = currentReader.readLine()) != null) {
						/** parse line */
						String[] values = line.split(SQConfig.sepStrForKeyValue)[1].split(SQConfig.sepStrForRecord);
						int ppid = Integer.valueOf(values[0]);
						for (int i = 1; i < num_independent_dims * 4 + 1; i++) {
							partition_store[ppid][i-1] = Float.valueOf(values[i]);
							
						}					
//						for (int i = 1; i < num_dims * 4 + 1; i++) {
//							partition_store[ppid][i - 1] = Float.valueOf(values[i]);
//						}
					}
					currentReader.close();
				} else {
					throw new Exception("the file is not found .");
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			/** get configuration from file */
//			num_dims = conf.getInt(SQConfig.strDimExpression, 2);
			String independentDimStr = conf.get(SQConfig.strIndependentDim);
			String[] independentSplitStr = independentDimStr.split(",");
			num_independent_dims = independentSplitStr.length;
			cell_num = conf.getInt(SQConfig.strNumOfSmallCells, 501);
			domains = new float[2];
			domains[0] = conf.getFloat(SQConfig.strDomainMin, 0.0f);
			domains[1] = conf.getFloat(SQConfig.strDomainMax, 10001.0f);
			smallRange = (int) Math.ceil((domains[1] - domains[0]) / cell_num);
			cell_store = new CellStore[(int) Math.pow(cell_num, num_independent_dims)];
			di_numBuckets = conf.getInt(SQConfig.strNumOfPartitions, 2);
			partition_store = new float[(int) Math.pow(di_numBuckets, num_independent_dims)][num_independent_dims * 4];
			maxLimitSupporting = conf.getInt(SQConfig.strMaxLimitSupportingArea, 5000);
			for (int i = 0; i < cell_store.length; i++)
				cell_store[i] = new CellStore(i);
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

					System.out.println("Start phase files");
					for (int i = 0; i < stats.length; ++i) {
						if (!stats[i].isDirectory()) {
							/** parse file */
							parseFile(fs, stats[i].getPath().toString());
						}
					}
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files");
			}
			System.out.println("Read file complete");

		}

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (int i = 0; i < Math.pow(di_numBuckets, num_independent_dims); i++) {
				System.out.println("i = " + i);
				dealEachPartition(i);
			}

			for (int i = 0; i < cell_store.length; i++) {
				if (cell_store[i].core_partition_id >= 0)
					context.write(NullWritable.get(), new Text(cell_store[i].printCellStoreWithSupport()));
				else
					System.out.println("Cannot find core partition for this cell:" + i);
			}
			System.out.println("Total Exceed Partitions: " + countExceedPartitions);

		}

		public void dealEachPartition(int indexOfPartition) {
			float[] partitionSize = partition_store[indexOfPartition];

			// assign core cells
			int[] indexes = new int[num_independent_dims * 2];
			int multiple = 1;
			for (int i = 0; i < num_independent_dims * 2; i = i + 2) {
				indexes[i] = (int) ((partitionSize[i] / smallRange));
				indexes[i + 1] = (int) ((partitionSize[i + 1] / smallRange));
				if (indexes[i] == indexes[i + 1]) {
					System.out.println("Cannot interprete this partition, contains nothing: " + indexOfPartition);
					return;
				}
				// System.out.print(indexes[i] + "," + indexes[i + 1] + ",");
				multiple *= (indexes[i + 1] - indexes[i]);
			}
			// System.out.println();

			ArrayList<String> previousList = new ArrayList<String>();
			ArrayList<String> newList = new ArrayList<String>();

			for (int i = 0; i < num_independent_dims; i++) {
				previousList.addAll(newList);
				newList.clear();
				int beginIndex = indexes[2 * i];
				int endIndex = indexes[2 * i + 1];
				for (int j = beginIndex; j < endIndex; j++) {
					if (previousList.size() == 0)
						newList.add(j + ",");
					else {
						for (int k = 0; k < previousList.size(); k++) {
							newList.add(previousList.get(k) + j + ",");
						}
					}
				}
				previousList.clear();
			}
			for (int i = 0; i < newList.size(); i++) {
				int cellId = CellStore.ComputeCellStoreId(newList.get(i).substring(0, newList.get(i).length() - 1),
						num_independent_dims, cell_num);
				// if(cell_store[cellId].core_partition_id>=0)
				// System.out.println("Already set to one core partition?
				// Why???");
				cell_store[cellId].core_partition_id = indexOfPartition;
			}

			// assign supporitng cells(dim *2 parts)
			int[] supportCellsSize = new int[num_independent_dims * 2];
			boolean flagExceed = false;
			for (int i = num_independent_dims * 2; i < num_independent_dims * 4; i++) {
				if (partitionSize[i] >= maxLimitSupporting) {
					System.out.println("Exceed max limit: " + partitionSize[i]);
					flagExceed = true;
					partitionSize[i] = maxLimitSupporting;
				}
			}
			if (flagExceed)
				countExceedPartitions++;
			for (int i = 0; i < num_independent_dims * 2; i++) {
				supportCellsSize[i] = (int) (Math.ceil(partitionSize[i + num_independent_dims * 2] / smallRange));
			}

			int[] newindexes = new int[num_independent_dims * 2];
			for (int i = 0; i < num_independent_dims; i++) {
				newindexes[2 * i] = Math.max(0, indexes[2 * i] - supportCellsSize[2 * i]);
				newindexes[2 * i + 1] = Math.min(cell_num, indexes[2 * i + 1] + supportCellsSize[2 * i + 1]);
				// System.out.print(newindexes[2 * i] + "," + newindexes[2 * i +
				// 1] + ",");
			}
			// System.out.println();

			ArrayList<String> finalSupportList = new ArrayList<String>();

			for (int i = 0; i < num_independent_dims; i++) {
				previousList.clear();
				newList.clear();
				if (indexes[2 * i] != newindexes[2 * i]) {
					for (int j = 0; j < num_independent_dims; j++) {
						previousList.addAll(newList);
						newList.clear();
						int beginIndex;
						int endIndex;
						if (i == j) {
							beginIndex = newindexes[2 * i];
							endIndex = indexes[2 * i];
						} else {
							beginIndex = newindexes[2 * j];
							endIndex = newindexes[2 * j + 1];
						}
						for (int k = beginIndex; k < endIndex; k++) {
							if (previousList.size() == 0)
								newList.add(k + ",");
							else {
								for (int mm = 0; mm < previousList.size(); mm++) {
									newList.add(previousList.get(mm) + k + ",");
								}
							}
						}
						previousList.clear();
					} // end iterator
					finalSupportList.addAll(newList);
				} // end dealing with the first item
				previousList.clear();
				newList.clear();
				if (indexes[2 * i + 1] != newindexes[2 * i + 1]) {
					for (int j = 0; j < num_independent_dims; j++) {
						previousList.addAll(newList);
						newList.clear();
						int beginIndex;
						int endIndex;
						if (i == j) {
							beginIndex = indexes[2 * i + 1];
							endIndex = newindexes[2 * i + 1];
						} else {
							beginIndex = newindexes[2 * j];
							endIndex = newindexes[2 * j + 1];
						}
						for (int k = beginIndex; k < endIndex; k++) {
							if (previousList.size() == 0)
								newList.add(k + ",");
							else {
								for (int mm = 0; mm < previousList.size(); mm++) {
									newList.add(previousList.get(mm) + k + ",");
								}
							}
						}
						previousList.clear();
					} // end iterator
					finalSupportList.addAll(newList);
				} // end dealing with the second item
			}
			// System.out.println("Size:" + finalSupportList.size());
			for (int i = 0; i < finalSupportList.size(); i++) {
				// System.out.println(finalSupportList.get(i));
				int cellId = CellStore.ComputeCellStoreId(
						finalSupportList.get(i).substring(0, finalSupportList.get(i).length() - 1), num_independent_dims, cell_num);
				cell_store[cellId].support_partition_id.add(indexOfPartition);
			}

		}

	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "Count Cells for supporting area");

		job.setJarByClass(DistributeCalSupportCellsOPT.class);
		job.setMapperClass(DistributedSupportCellMapper.class);
		job.setReducerClass(DistributedSupportCellReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		String strFSName = conf.get("fs.default.name");
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.strIndexFilePath)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strKnnCellsOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strKnnCellsOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strKnnPartitionPlan)));

		/** print job parameter */
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, 10));
		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) throws Exception {
		DistributeCalSupportCellsOPT dcsc = new DistributeCalSupportCellsOPT();
		dcsc.run(args);
	}
}
