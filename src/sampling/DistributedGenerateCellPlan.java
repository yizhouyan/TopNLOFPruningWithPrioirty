package sampling;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import sampling.CellStore;
import util.SQConfig;

public class DistributedGenerateCellPlan {

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

		/**
		 * The dimension of data (set by user, now only support dimension of 2,
		 * if change to 3 or more, has to change some codes)
		 */
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

						String[] values = line.split(SQConfig.sepStrForRecord);
						int ppid = Integer.valueOf(values[0]);
						for (int i = 1; i < num_independent_dims * 2 + 1; i++) {
							partition_store[ppid][i - 1] = Float.valueOf(values[i]);
						}
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
			partition_store = new float[(int) Math.pow(di_numBuckets, num_independent_dims)][num_independent_dims * 2];

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
						if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("pp-r")) {
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
				dealEachPartition(i);
			}

			for (int i = 0; i < cell_store.length; i++) {
				if (cell_store[i].core_partition_id >= 0)
					context.write(NullWritable.get(), new Text(cell_store[i].printCellStoreBasic()));
				else
					System.out.println("Cannot find core partition for this cell:" + i);
			}

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
				int cellId = CellStore.ComputeCellStoreId(newList.get(i), num_independent_dims, cell_num);
//				if(cell_store[cellId].core_partition_id>=0)
//					System.out.println("Already set to one core partition? Why???");
				cell_store[cellId].core_partition_id = indexOfPartition;
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

		job.setJarByClass(DistributedGenerateCellPlan.class);
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
		fs.delete(new Path(conf.get(SQConfig.strCellsOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strCellsOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strPartitionPlanOutput)));

		/** print job parameter */
		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) throws Exception {
		DistributedGenerateCellPlan dcsc = new DistributedGenerateCellPlan();
		dcsc.run(args);
	}
}
