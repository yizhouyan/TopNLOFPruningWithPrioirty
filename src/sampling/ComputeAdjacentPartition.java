package sampling;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import util.SQConfig;

public class ComputeAdjacentPartition {
	private int[] indexOfIndependentDims;
	private int num_independentDim;
	/**
	 * block list, which saves each block's info including start & end positions
	 * on each dimension. print for speed up "mapping"
	 */
	private float[][] partition_store;
	/**
	 * Number of desired partitions in each dimension (set by user), for Data
	 * Driven partition
	 */
	private int num_partition;

	private ArrayList<Integer> [] partitionAdj;
	
	public ComputeAdjacentPartition() {
	}

	private void parsePartitionFile(FileSystem fs, String filename) {
		try {
			// check if the file exists
			Path path = new Path(filename);
			if (fs.exists(path)) {
				FSDataInputStream currentStream;
				BufferedReader currentReader;
				currentStream = fs.open(path);
				currentReader = new BufferedReader(new InputStreamReader(currentStream));
				String line;
				int i;
				while ((line = currentReader.readLine()) != null) {
					/** parse line */
					String[] splitsStr = line.split(SQConfig.sepStrForRecord);
					int tempid = Integer.parseInt(splitsStr[0]);
					for (int j = 1; j < num_independentDim * 2 + 1; j++) {
						partition_store[tempid][j - 1] = Float.valueOf(splitsStr[j]);
					}
//					System.out.println(tempid + "," + partition_store[tempid][0] + "," + partition_store[tempid][1] + ","
//					+ partition_store[tempid][2] + ","+ partition_store[tempid][3]);
				}
				currentReader.close();
			} else {
				throw new Exception("the file is not found .");
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private void parseDimFile(FileSystem fs, String filename) {
		try {
			// check if the file exists
			Path path = new Path(filename);
			if (fs.exists(path)) {
				FSDataInputStream currentStream;
				BufferedReader currentReader;
				currentStream = fs.open(path);
				currentReader = new BufferedReader(new InputStreamReader(currentStream));

				// get number of independent dims
				num_independentDim = Integer.parseInt(currentReader.readLine());
				indexOfIndependentDims = new int[num_independentDim];

				// get independent dims
				String independentDimString = currentReader.readLine();
				String[] independentDimSplits = independentDimString.split(",");
				for (int i = 0; i < independentDimSplits.length; i++) {
					indexOfIndependentDims[i] = Integer.parseInt(independentDimSplits[i]);
				}

				// get partition number
				num_partition = Integer.parseInt(currentReader.readLine());
				partition_store = new float[num_partition][num_independentDim * 2];
				partitionAdj = new ArrayList[num_partition];
				for(int i = 0; i< num_partition; i++){
					partitionAdj[i] = new ArrayList<Integer>();
				}
				currentReader.close();
			} else {
				throw new Exception("the file is not found .");
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void computeAdjPartitionForEach(String input_partition_dir, String input_dim_dir, String output)
			throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);

		/** parse dim directory file */
		FileStatus[] stats = fs.listStatus(new Path(input_dim_dir));

		for (int i = 0; i < stats.length; ++i) {
			if (!stats[i].isDirectory()) {
				/** parse file */
				parseDimFile(fs, stats[i].getPath().toString());
			}
		}

		/** parse file */
		FileStatus[] stats2 = fs.listStatus(new Path(input_partition_dir));

		for (int i = 0; i < stats2.length; ++i) {
			if (!stats2[i].isDirectory() && stats2[i].getPath().toString().contains("pp-r")) {
				/** parse file */
				parsePartitionFile(fs, stats2[i].getPath().toString());
			}
		}

		/** compute adjacent partitions */
		generateAdjacentPartitions();
		/** output to HDFS */
		writeToHDFS(fs, output);
		fs.close();
	}

	private boolean checkAdjacencyBetweenTwoPartitions(int ii, int jj){
		for(int d = 0; d< num_independentDim ;d++){
			float min1 = partition_store[ii][2* d];
			float max1 = partition_store[ii][2 * d +1];
			float min2 = partition_store[jj][2 * d];
			float max2 = partition_store[jj][2 * d +1];
			if(Math.min(max1,max2)-Math.max(min1, min2) < 0)
				return false;
		}
		return true;
	}
	private void generateAdjacentPartitions(){
		for (int i = 0; i< num_partition; i++)
			for(int j = i+1; j < num_partition; j++){
				// find if there is any correlation with partition i and partition j
				if(checkAdjacencyBetweenTwoPartitions(i,j)){
					partitionAdj[i].add(j);
					partitionAdj[j].add(i);
				}
			}
	}
	private void writeToHDFS(FileSystem fs, String output_dir) {
		double sumOfDiameter = 0;
		try {
			String filename = output_dir + "/adjacency.csv";
			Path path = new Path(filename);
			System.out.println("output path:" + path);
			FSDataOutputStream currentStream;
			currentStream = fs.create(path, true);
			String line;
			
			/** writhe the correlation information */
			for(int i = 0 ;i< num_partition; i++){
				line = i + ":";
				for(Integer j: partitionAdj[i])
					line += j + ",";
				if(line.endsWith(","))
					line = line.substring(0, line.length()-1);
				line += "\n";
				currentStream.writeBytes(line);
			}
			currentStream.close();
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		try {
			// String input = args[0];
			String partitionInput = conf.get(SQConfig.strPartitionPlanOutput);
			String dimInfoInput = conf.get(SQConfig.strDimCorrelationOutput);
			String output = conf.get(SQConfig.strPartitionAjacencyOutput);

			ComputeAdjacentPartition ap = new ComputeAdjacentPartition();
			long begin = System.currentTimeMillis();
			ap.computeAdjPartitionForEach(partitionInput, dimInfoInput, output);
			long end = System.currentTimeMillis();
			long second = (end - begin) / 1000;
			System.err.println("Compute Adjacent Partition takes " + second + " seconds");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
