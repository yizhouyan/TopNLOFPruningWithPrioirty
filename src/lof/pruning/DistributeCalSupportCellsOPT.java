package lof.pruning;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

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
		public static int count= 1;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//	System.out.println(value.toString());
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
		private static float[][] domains;

		/** size of each small buckets */
		private static int smallRange;

		/**
		 * The dimension of data (set by user, now only support dimension of 2,
		 * if change to 3 or more, has to change some codes)
		 */
		private static int num_dims = 2;

		/**
		 * Number of desired partitions in each dimension (set by user), for
		 * Data Driven partition
		 */
		private static int[] di_numBuckets; ////////////////////////////////////

		/**
		 * block list, which saves each block's info including start & end
		 * positions on each dimension. print for speed up "mapping"
		 */
		private static float[][] partition_store;

		/** save each small buckets. in order to speed up mapping process */
		private static CellStore[][] cell_store;

		private static int maxLimitSupporting;
	//	private static double maxOverlaps = 0;


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
						for (int i = 1; i < values.length; i++) {
							partition_store[ppid][i - 1] = Float.valueOf(values[i]);
						}
//						maxOverlaps = Math.max(partition_store[ppid][num_dims * 2], maxOverlaps);
//						maxOverlaps = Math.max(partition_store[ppid][num_dims * 2 + 1], maxOverlaps);
//						maxOverlaps = Math.max(partition_store[ppid][num_dims * 2 + 2], maxOverlaps);
//						maxOverlaps = Math.max(partition_store[ppid][num_dims * 2 + 3], maxOverlaps);
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
			maxLimitSupporting = conf.getInt(SQConfig.strMaxLimitSupportingArea, 5000);
			for(int i = 0; i< cell_num; i++)
				for(int j = 0; j< cell_num; j++)
					cell_store[i][j] = new CellStore(i * smallRange, ((i + 1) * smallRange), j * smallRange,
							((j + 1) * smallRange));
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
			for (int i = 0; i < di_numBuckets[0] * di_numBuckets[1]; i++) {
				System.out.println("i = " + i);
				dealEachPartition(i);
			}
			
				for (int i = 0; i < cell_num; i++) {
					
					for (int j = 0; j < cell_num; j++) {
						context.write(NullWritable.get(), new Text(cell_store[i][j].printCellStoreWithSupport()));
					}
				}
			
		}
		
		public void dealEachPartition(int indexOfPartition) {
			float[] partitionSize = partition_store[indexOfPartition];
			// assign core cells
			int[] indexes = new int[4];
			for (int i = 0; i < 4; i++) {
				indexes[i] = (int) ((partitionSize[i] / smallRange));
				// if(indexes[i]<0)
				// indexes[i] = 0;
				// if(indexes[i] >= cell_num)
				// indexes[i] = cell_num-1;
			}
			for (int i = indexes[0]; i < indexes[1]; i++) {
				for (int j = indexes[2]; j < indexes[3]; j++) {
					cell_store[i][j].core_partition_id = indexOfPartition;
				}
			}
			// assign supporting cells(4 parts)
			int []supportCellsSize = new int [4];
			for (int i = 4; i < 8; i++) {
				if (partitionSize[i] >= maxLimitSupporting)
					partitionSize[i] = maxLimitSupporting;
			}
			for (int i = 0; i< 4;i++){
				supportCellsSize[i] = (int) (Math.ceil(partitionSize[i+4] / smallRange));
			}
			int[] newindexes = new int[4];
			newindexes[0] = Math.max(0, indexes[0] - supportCellsSize[0]);
			newindexes[1] = Math.min(cell_num, indexes[1] + supportCellsSize[1]);
			newindexes[2] = Math.max(0, indexes[2] - supportCellsSize[2]);
			newindexes[3] = Math.min(cell_num, indexes[3] + supportCellsSize[3]);

			for (int j = newindexes[2]; j < indexes[2]; j++) {
				for (int i = newindexes[0]; i < newindexes[1]; i++) {
					cell_store[i][j].support_partition_id.add(indexOfPartition);
				}
			}
			
			for(int j = indexes[3]; j < newindexes[3] ; j++){
				for (int i = newindexes[0]; i < newindexes[1]; i++) {
					cell_store[i][j].support_partition_id.add(indexOfPartition);
				}
			}
			for(int i = newindexes[0] ; i< indexes[0] ;i++){
				for(int j = indexes[2]; j< indexes[3] ;j++){
					cell_store[i][j].support_partition_id.add(indexOfPartition);
				}
			}
			for(int i = indexes[1] ; i< newindexes[1] ;i++){
				for(int j = indexes[2]; j< indexes[3] ;j++){
					cell_store[i][j].support_partition_id.add(indexOfPartition);
				}
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
