package lof.pruning.firstknn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import metricspace.MetricKey;
import util.SQConfig;

public class PriorityPartitioner extends Partitioner<MetricKey, Text> implements Configurable {

	public HashMap<Integer, Integer> partitionToReducer = new HashMap<Integer, Integer>();

	@Override
	public int getPartition(MetricKey key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		int reducerNum = 0;
		try {
			reducerNum = partitionToReducer.get(key.pid);
		} catch (Exception e) {
			System.out.println("Trying to find the reducer for partition: " + key.pid);
		}
		return reducerNum;
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		/** parse files in the cache */
		try {
			String strFSName = conf.get("fs.default.name");
			FileSystem fs = FileSystem.get(conf);
			URI path = new URI(strFSName + conf.get(SQConfig.strPartitionPlanOutput));
			String filename = path.toString();
			// System.err.println("File Name: " + filename);
			FileStatus[] stats = fs.listStatus(new Path(filename));
			for (int i = 0; i < stats.length; ++i) {
				if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("partitionAssignment")) {
					System.out.println("Reading partition assignment plan from " + stats[i].getPath().toString());
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

			System.out.println("In Partitioner-----Partition size: " + partitionToReducer.size());
		} catch (IOException ioe) {
			System.err.println("Caught exception while getting cached files");
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}