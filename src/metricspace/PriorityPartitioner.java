package metricspace;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper.Context;

import sampling.CellStore;
import util.SQConfig;

public class PriorityPartitioner extends Partitioner<MetricKey, Text> {

	public static HashMap<Integer, Integer> partitionToReducer = new HashMap<Integer, Integer>();
	
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		/** parse files in the cache */
		try {
			URI[] cacheFiles = context.getCacheArchives();
			for (URI path : cacheFiles) {
				String filename = path.toString();
				FileSystem fs = FileSystem.get(conf);
				System.err.println("File Name: " + filename);
				FileStatus[] stats = fs.listStatus(new Path(filename));
				for (int i = 0; i < stats.length; ++i) {
					if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("partitionAssignment")) {
						System.out.println("Reading partition assignment plan from "+ stats[i].getPath().toString());
						FSDataInputStream currentStream;
						BufferedReader currentReader;
						currentStream = fs.open(stats[i].getPath());
						currentReader = new BufferedReader(new InputStreamReader(currentStream));
						String line;
						while ((line = currentReader.readLine()) != null) {
							/** parse line */
							String[] splitsStr = line.split(SQConfig.sepStrForRecord);
							int partitionId = Integer.parseInt(splitsStr[0]);
							int reducerId = Integer.parseInt(splitsStr[1]);
							partitionToReducer.put(partitionId, reducerId);
							// System.out.println("partitionplan : "+
							// partition_store[tempid][3]);
						}
					} 
				} // end for (int i = 0; i < stats.length; ++i)
			} // end for (URI path : cacheFiles)
			System.out.println("In Partitioner-----Partition size: " + partitionToReducer.size());
		} catch (IOException ioe) {
			System.err.println("Caught exception while getting cached files");
		}
	}

	@Override
	public int getPartition(MetricKey key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		return partitionToReducer.get(key.pid);
	}
	
//	@Override
//	public int getPartition(MetricKey key, Text value, int numPartitions) {
//		// TODO Auto-generated method stub
//		return Math.abs(key.pid * 127) % numPartitions;
//	}

}
