package lof.pruning;

//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.net.URI;
//import java.net.URISyntaxException;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Stack;
//
//import org.apache.hadoop.conf.Configurable;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Partitioner;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
//import org.apache.hadoop.util.GenericOptionsParser;
//
//import lof.pruning.CalKdistanceFirstMultiDimSortByMax.Counters;
//import lof.pruning.firstknn.LargeCellStore;
//import metricspace.IMetric;
//import metricspace.IMetricSpace;
//import metricspace.MetricKey;
//import metricspace.MetricObject;
//import metricspace.MetricSpaceUtility;
//import metricspace.PriorityComparator;
//import metricspace.PriorityGroupComparator;
////import metricspace.PriorityPartitioner;
//import metricspace.Record;
//import metricspace.SafeArea;
//import sampling.CellStore;
//import util.PriorityQueue;
//import util.SQConfig;
//
public class CalKdistanceFirstMultiDimSortByMax {
	// /**
	// * default Map class.
	// *
	// * @author Yizhou Yan
	// * @version Nov 26, 2016
	// */
	//
	// /** number of object pairs to be computed */
	// static enum Counters {
	// CellPrunedPoints, LRDPrunedPoints, FinalPrunedPoints, TotalPoints,
	// NeedOutputPoints,
	// }
	//
	// public static class FirstKNNFinderMapper extends Mapper<LongWritable,
	// Text, IntWritable, Text> {
	// /**
	// * The dimension of data (set by user, now only support dimension of 2,
	// * if change to 3 or more, has to change some codes)
	// */
	// private static int num_dims = 2;
	// /**
	// * number of small cells per dimension: when come with a node, map to a
	// * range (divide the domain into small_cell_num_per_dim) (set by user)
	// */
	// private static int cell_num = 501;
	//
	// /** The domains. (set by user) */
	// private static float[] domains;
	// /** size of each small buckets */
	// private static int smallRange;
	//
	// private int[] indexOfIndependentDims;
	// private int num_independentDim;
	// private int num_correlatedDim;
	// private int[] indexOfCorrelatedDims;
	// /**
	// * block list, which saves each block's info including start & end
	// * positions on each dimension. print for speed up "mapping"
	// */
	// private static float[][] partition_store;
	// // private static double[] partition_priority;
	// /** save each small buckets. in order to speed up mapping process */
	// private static CellStore[] cell_store;
	// /**
	// * Number of desired partitions in each dimension (set by user), for
	// * Data Driven partition
	// */
	// // private static int di_numBuckets;
	// private static int num_partitions;
	// private static ArrayList<Integer>[] partitionAdj;
	//
	// protected void setup(Context context) throws IOException,
	// InterruptedException {
	// Configuration conf = context.getConfiguration();
	// /** get configuration from file */
	// num_dims = conf.getInt(SQConfig.strDimExpression, 2);
	// num_independentDim = conf.getInt(SQConfig.strIndependentDimCount, 2);
	// cell_num = conf.getInt(SQConfig.strNumOfSmallCells, 501);
	// domains = new float[2];
	// domains[0] = conf.getFloat(SQConfig.strDomainMin, 0.0f);
	// domains[1] = conf.getFloat(SQConfig.strDomainMax, 10001.0f);
	// smallRange = (int) Math.ceil((domains[1] - domains[0]) / cell_num);
	// cell_store = new CellStore[(int) Math.pow(cell_num, num_independentDim)];
	// // di_numBuckets = conf.getInt(SQConfig.strNumOfPartitions, 2);
	//
	// // partition_priority = new double[(int) Math.pow(di_numBuckets,
	// // num_independentDim)];
	// /** parse files in the cache */
	// try {
	// URI[] cacheFiles = context.getCacheArchives();
	//
	// if (cacheFiles == null || cacheFiles.length < 4) {
	// System.out.println("not enough cache files");
	// return;
	// }
	// // first read in dim information
	// for (URI path : cacheFiles) {
	// String filename = path.toString();
	// FileSystem fs = FileSystem.get(conf);
	//
	// FileStatus[] stats = fs.listStatus(new Path(filename));
	// for (int i = 0; i < stats.length; ++i) {
	// if (!stats[i].isDirectory() &&
	// stats[i].getPath().toString().contains("dc-r")) {
	// System.out.println("Reading dim correlation from " +
	// stats[i].getPath().toString());
	// FSDataInputStream currentStream;
	// BufferedReader currentReader;
	// currentStream = fs.open(stats[i].getPath());
	// currentReader = new BufferedReader(new InputStreamReader(currentStream));
	//
	// // get number of independent dims
	// currentReader.readLine();
	// num_correlatedDim = num_dims - num_independentDim;
	// indexOfIndependentDims = new int[num_independentDim];
	// indexOfCorrelatedDims = new int[num_correlatedDim];
	//
	// // get independent dims
	// String independentDimString = currentReader.readLine();
	// String[] independentDimSplits = independentDimString.split(",");
	// for (int ii = 0; ii < independentDimSplits.length; ii++) {
	// indexOfIndependentDims[ii] = Integer.parseInt(independentDimSplits[ii]);
	// }
	//
	// // get correlated dims
	// String correlatedDimString = currentReader.readLine();
	// String[] correlatedDimSplits = correlatedDimString.split(",");
	// for (int ii = 0; ii < correlatedDimSplits.length; ii++) {
	// indexOfCorrelatedDims[ii] = Integer.parseInt(correlatedDimSplits[ii]);
	// }
	// // get partition number
	// num_partitions = Integer.parseInt(currentReader.readLine());
	// partition_store = new float[num_partitions][num_dims * 2];
	// partitionAdj = new ArrayList[num_partitions];
	// for (int ii = 0; ii < num_partitions; ii++) {
	// partitionAdj[ii] = new ArrayList<Integer>();
	// }
	// currentReader.close();
	// currentStream.close();
	// }
	// }
	// } // end for URI
	//
	// // then read in others
	// for (URI path : cacheFiles) {
	// String filename = path.toString();
	// FileSystem fs = FileSystem.get(conf);
	//
	// FileStatus[] stats = fs.listStatus(new Path(filename));
	// for (int i = 0; i < stats.length; ++i) {
	// if (!stats[i].isDirectory() &&
	// stats[i].getPath().toString().contains("pp")) {
	// System.out.println("Reading partition plan from " +
	// stats[i].getPath().toString());
	// FSDataInputStream currentStream;
	// BufferedReader currentReader;
	// currentStream = fs.open(stats[i].getPath());
	// currentReader = new BufferedReader(new InputStreamReader(currentStream));
	// String line;
	// while ((line = currentReader.readLine()) != null) {
	// /** parse line */
	//
	// String[] splitsStr = line.split(SQConfig.sepStrForRecord);
	// int tempid = Integer.parseInt(splitsStr[0]);
	// for (int j = 0; j < num_dims * 2; j++) {
	// partition_store[tempid][j] = Float.parseFloat(splitsStr[j + 1]);
	// }
	// // partition_priority[tempid] =
	// // Double.parseDouble(splitsStr[num_dims * 2 +
	// // 1]);
	// // System.out.println("partitionplan : "+
	// // partition_store[tempid][3]);
	// }
	// currentReader.close();
	// currentStream.close();
	// } else if (!stats[i].isDirectory() &&
	// stats[i].getPath().toString().contains("part")) {
	// System.out.println("Reading cells for partitions from " +
	// stats[i].getPath().toString());
	// FSDataInputStream currentStream;
	// BufferedReader currentReader;
	// currentStream = fs.open(stats[i].getPath());
	// currentReader = new BufferedReader(new InputStreamReader(currentStream));
	// String line;
	// while ((line = currentReader.readLine()) != null) {
	// /** parse line */
	// String[] items = line.split(SQConfig.sepStrForRecord);
	// if (items.length == 2) {
	// int cellId = Integer.valueOf(items[0]);
	// int corePartitionId = Integer.valueOf(items[1].substring(2));
	// cell_store[cellId] = new CellStore(cellId, corePartitionId);
	// }
	// } // end while
	// currentReader.close();
	// currentStream.close();
	// } else if (!stats[i].isDirectory() &&
	// stats[i].getPath().toString().contains("adjacency.csv")) {
	// System.out.println("Reading partition adjacency from " +
	// stats[i].getPath().toString());
	// FSDataInputStream currentStream;
	// BufferedReader currentReader;
	// currentStream = fs.open(stats[i].getPath());
	// currentReader = new BufferedReader(new InputStreamReader(currentStream));
	// String line;
	// while ((line = currentReader.readLine()) != null) {
	// /** parse line */
	// String[] strSplits = line.split(":");
	// int tempId = Integer.parseInt(strSplits[0]);
	// if (strSplits[1].length() != 0) {
	// String[] tempValues = strSplits[1].split(SQConfig.sepStrForRecord);
	// for (String str : tempValues) {
	// partitionAdj[tempId].add(Integer.parseInt(str));
	// }
	// }
	// } // end while
	// currentReader.close();
	// currentStream.close();
	// }
	// } // end for (int i = 0; i < stats.length; ++i)
	// } // end for (URI path : cacheFiles)
	//
	// } catch (IOException ioe) {
	// System.err.println("Caught exception while getting cached files");
	// }
	// // end parse file
	// // init correlated domain to [+inf -inf]
	//
	// for (int i = 0; i < num_partitions; i++) {
	// for (int j = 0; j < num_correlatedDim; j++) {
	// partition_store[i][indexOfCorrelatedDims[j] * 2] =
	// Float.POSITIVE_INFINITY;
	// partition_store[i][indexOfCorrelatedDims[j] * 2 + 1] =
	// Float.NEGATIVE_INFINITY;
	// }
	// }
	// int tempid = 0;
	// // System.out.println(tempid + "," + partition_store[tempid][0] +
	// // "," + partition_store[tempid][1] + ","
	// // + partition_store[tempid][2] + "," + partition_store[tempid][3] +
	// // "," + partition_store[tempid][4]
	// // + "," + partition_store[tempid][5] + "," +
	// // partition_store[tempid][6] + ","
	// // + partition_store[tempid][7]);
	// }
	//
	// public void map(LongWritable key, Text value, Context context) throws
	// IOException, InterruptedException {
	// // Variables
	// float[] crds = new float[num_independentDim]; // coordinates of one
	// // input
	// // data
	// String[] splitStr = value.toString().split(SQConfig.sepStrForRecord);
	// // parse raw input data into coordinates/crds
	// for (int i = 0; i < num_independentDim; i++) {
	// // System.out.println(value.toString());
	// crds[i] = Float.parseFloat(splitStr[indexOfIndependentDims[i] + 1]);
	// }
	// // find which cell the point in
	// int cell_id = CellStore.ComputeCellStoreId(crds, num_independentDim,
	// cell_num, smallRange);
	// if (cell_id < 0)
	// return;
	// // partition id = core area partition id
	// int blk_id = cell_store[cell_id].core_partition_id;
	// if (blk_id < 0)
	// return;
	// // context.write(new MetricKey(blk_id, partition_priority[blk_id]),
	// // value);
	// for (int i = 0; i < num_correlatedDim; i++) {
	// float tempNum = Float.parseFloat(splitStr[indexOfCorrelatedDims[i] + 1]);
	// if (tempNum < partition_store[blk_id][indexOfCorrelatedDims[i] * 2])
	// partition_store[blk_id][indexOfCorrelatedDims[i] * 2] = tempNum;
	// if (tempNum > partition_store[blk_id][indexOfCorrelatedDims[i] * 2 + 1])
	// partition_store[blk_id][indexOfCorrelatedDims[i] * 2 + 1] = tempNum;
	// }
	// // context.write(new MetricKey(blk_id, 1), value);
	// context.write(new IntWritable(blk_id), value);
	// }
	//
	// public String wrapUpPartitionInformationToString(int i) {
	// String outputStr = "PInfo:" + i + ",";
	// for (int j = 0; j < partition_store[i].length; j++) {
	// outputStr += partition_store[i][j] + ",";
	// }
	// outputStr = outputStr.substring(0, outputStr.length() - 1);
	// return outputStr;
	// }
	//
	// public void cleanup(Context context) throws IOException,
	// InterruptedException {
	// // output partition information for each adjacent partition
	// for (int i = 0; i < num_partitions; i++) {
	// // context.write(new MetricKey(i, 10), new
	// // Text(wrapUpPartitionInformationToString(i)));
	// context.write(new IntWritable(i), new
	// Text(wrapUpPartitionInformationToString(i)));
	// for (Integer adjPartition : partitionAdj[i]) {
	// // context.write(new MetricKey(i, 10), new
	// // Text(wrapUpPartitionInformationToString(adjPartition)));
	// context.write(new IntWritable(i), new
	// Text(wrapUpPartitionInformationToString(adjPartition)));
	// }
	// }
	// }
	// }
	//
	// // public static class PriorityPartitioner extends Partitioner<MetricKey,
	// // Text> implements Configurable {
	// //
	// // public HashMap<Integer, Integer> partitionToReducer = new
	// // HashMap<Integer, Integer>();
	// //
	// // @Override
	// // public int getPartition(MetricKey key, Text value, int numPartitions)
	// {
	// // // TODO Auto-generated method stub
	// // int reducerNum = 0;
	// // try {
	// // reducerNum = partitionToReducer.get(key.pid);
	// // } catch (Exception e) {
	// // System.out.println("Trying to find the reducer for partition: " +
	// // key.pid);
	// // }
	// // return reducerNum;
	// // }
	// //
	// // @Override
	// // public Configuration getConf() {
	// // // TODO Auto-generated method stub
	// // return null;
	// // }
	// //
	// // @Override
	// // public void setConf(Configuration conf) {
	// // // TODO Auto-generated method stub
	// // /** parse files in the cache */
	// // try {
	// // String strFSName = conf.get("fs.default.name");
	// // FileSystem fs = FileSystem.get(conf);
	// // URI path = new URI(strFSName +
	// // conf.get(SQConfig.strPartitionPlanOutput));
	// // String filename = path.toString();
	// // // System.err.println("File Name: " + filename);
	// // FileStatus[] stats = fs.listStatus(new Path(filename));
	// // for (int i = 0; i < stats.length; ++i) {
	// // if (!stats[i].isDirectory() &&
	// // stats[i].getPath().toString().contains("partitionAssignment")) {
	// // // System.out.println("Reading partition assignment plan
	// // // from " + stats[i].getPath().toString());
	// // FSDataInputStream currentStream;
	// // BufferedReader currentReader;
	// // currentStream = fs.open(stats[i].getPath());
	// // currentReader = new BufferedReader(new
	// InputStreamReader(currentStream));
	// // String line;
	// // while ((line = currentReader.readLine()) != null) {
	// // /** parse line */
	// // String[] splitsStr = line.split(SQConfig.sepStrForRecord);
	// // int partitionId = Integer.parseInt(splitsStr[0]);
	// // int reducerId = Integer.parseInt(splitsStr[1]);
	// // partitionToReducer.put(partitionId, reducerId);
	// // // System.out.println("partitionplan : "+
	// // // partition_store[tempid][3]);
	// // }
	// // }
	// // } // end for (int i = 0; i < stats.length; ++i)
	// //
	// // // System.out.println("In Partitioner-----Partition size: " +
	// // // partitionToReducer.size());
	// // } catch (IOException ioe) {
	// // System.err.println("Caught exception while getting cached files");
	// // } catch (URISyntaxException e) {
	// // // TODO Auto-generated catch block
	// // e.printStackTrace();
	// // }
	// //
	// // }
	// // }
	//
	// /**
	// * @author yizhouyan
	// *
	// */
	// public static class FirstKNNFinderReducer extends Reducer<IntWritable,
	// Text, LongWritable, Text> {
	// /**
	// * The dimension of data
	// */
	// private static int num_dims = 2;
	// /** The domains. (set by user) */
	// private static float[] domains;
	// private static int K;
	// private IMetricSpace metricSpace = null;
	// private IMetric metric = null;
	// private MultipleOutputs<LongWritable, Text> mos;
	// private float thresholdLof = 10.0f;
	// private PriorityQueue topnLOF = new
	// PriorityQueue(PriorityQueue.SORT_ORDER_ASCENDING);
	// private int topNNumber = 100;
	// private float segmentLength = 10;
	// // private int countNumPartition = 0;
	//
	// /**
	// * get MetricSpace and metric from configuration
	// *
	// * @param conf
	// * @throws IOException
	// */
	// private void readMetricAndMetricSpace(Configuration conf) throws
	// IOException {
	// try {
	// metricSpace =
	// MetricSpaceUtility.getMetricSpace(conf.get(SQConfig.strMetricSpace));
	// metric = MetricSpaceUtility.getMetric(conf.get(SQConfig.strMetric));
	// metricSpace.setMetric(metric);
	// } catch (InstantiationException e) {
	// throw new IOException("InstantiationException");
	// } catch (IllegalAccessException e) {
	// e.printStackTrace();
	// throw new IOException("IllegalAccessException");
	// } catch (ClassNotFoundException e) {
	// e.printStackTrace();
	// throw new IOException("ClassNotFoundException");
	// }
	// }
	//
	// public void setup(Context context) throws IOException {
	// mos = new MultipleOutputs<LongWritable, Text>(context);
	// Configuration conf = context.getConfiguration();
	// readMetricAndMetricSpace(conf);
	// /** get configuration from file */
	// num_dims = conf.getInt(SQConfig.strDimExpression, 2);
	// domains = new float[2];
	// domains[0] = conf.getFloat(SQConfig.strDomainMin, 0.0f);
	// domains[1] = conf.getFloat(SQConfig.strDomainMax, 10001.0f);
	// K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
	//
	// thresholdLof = conf.getFloat(SQConfig.strLOFThreshold, 1.0f);
	// topNNumber = conf.getInt(SQConfig.strLOFTopNThreshold, 100);
	// } // end set up
	//
	// private MetricObject parseObject(int key, String strInput) {
	// int partition_id = key;
	// int offset = 0;
	// Object obj = metricSpace.readObject(strInput.substring(offset,
	// strInput.length()), num_dims);
	// return new MetricObject(partition_id, obj);
	// }
	//
	// public static float[] maxOfTwoFloatArray(float[] x, float[] y) {
	// float[] newArray = { Math.max(x[0], y[0]), Math.max(x[1], y[1]),
	// Math.max(x[2], y[2]),
	// Math.max(x[3], y[3]) };
	// return newArray;
	// }
	//
	// public static String floatArrayToString(float[] p) {
	// String str = "";
	// for (int i = 0; i < p.length; i++) {
	// str += p[i] + ",";
	// }
	// str = str.substring(0, str.length() - 1);
	// return str;
	// }
	//
	// public static float[] boundaryOfTwoPartitions(float[] p1, float[] p2) {
	// float[] boundary = new float[p1.length];
	// for (int i = 0; i < num_dims; i++) {
	// boundary[2 * i] = Math.max(p1[2 * i], p2[2 * i]);
	// boundary[2 * i + 1] = Math.min(p1[2 * i + 1], p2[2 * i + 1]);
	// }
	// return boundary;
	// }
	//
	// public float MinDistFromPointToBoundary(float[] boundary, float[]
	// pointCrds) {
	// float[] newPoint = new float[pointCrds.length];
	// float mindist = 0.0f;
	// for (int i = 0; i < pointCrds.length; i++) {
	// if (boundary[2 * i] >= pointCrds[i])
	// newPoint[i] = boundary[2 * i];
	// else if (boundary[2 * i + 1] <= pointCrds[i])
	// newPoint[i] = boundary[2 * i + 1];
	// else
	// newPoint[i] = pointCrds[i];
	// }
	// try {
	// mindist = metric.dist(new Record(0, pointCrds), new Record(0, newPoint));
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// return mindist;
	// }
	//
	// public float MaxDistFromPointToBoundary(float[] boundary, float[]
	// pointCrds) {
	// float maxDist = 0.0f;
	// float[] newPoint = new float[pointCrds.length];
	// for (int i = 0; i < pointCrds.length; i++) {
	// if (Math.abs(boundary[2 * i] - pointCrds[i]) > Math.abs(boundary[2 * i +
	// 1] - pointCrds[i]))
	// newPoint[i] = boundary[2 * i];
	// else
	// newPoint[i] = boundary[2 * i + 1];
	// }
	// // System.out.println("Point: " + floatArrayToString(pointCrds));
	// // System.out.println("Boundary: " + floatArrayToString(boundary));
	// // System.out.println("New Point: " + floatArrayToString(newPoint));
	// try {
	// maxDist = metric.dist(new Record(0, pointCrds), new Record(0, newPoint));
	// // System.out.println("Max: " + maxDist);
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// return maxDist;
	// }
	//
	// public float MaxDistFromPointListToBoundary(float[] boundary,
	// ArrayList<MetricObject> knnToBoundList) {
	// float maxDist = 0.0f;
	// for (MetricObject mo : knnToBoundList) {
	// maxDist = Math.max(maxDist, MaxDistFromPointToBoundary(boundary,
	// ((Record) mo.getObj()).getValue()));
	// }
	// return maxDist;
	// }
	//
	// public float computeKNNForBoundary(float[] boundary,
	// ArrayList<MetricObject> pointList) {
	// PriorityQueue knnToBound = new
	// PriorityQueue(PriorityQueue.SORT_ORDER_DESCENDING);
	// for (int i = 0; i < pointList.size(); i++) {
	// // compute min distance from point to boundary
	// float tempMaxDist = MaxDistFromPointToBoundary(boundary,
	// ((Record) pointList.get(i).getObj()).getValue());
	// if (knnToBound.size() < K) {
	// knnToBound.insert((long) i, tempMaxDist);
	// } else if (tempMaxDist < knnToBound.getPriority()) {
	// knnToBound.pop();
	// knnToBound.insert(i, tempMaxDist);
	// }
	// }
	// return knnToBound.getPriority();
	// }
	//
	// public void filterDataPoints(float[] boundary, ArrayList<MetricObject>
	// listOfPoints, float maxDist) {
	// for (MetricObject mo : listOfPoints) {
	// float[] crds = ((Record) mo.getObj()).getValue();
	// float minDistToB = MinDistFromPointToBoundary(boundary, crds);
	// // System.out.println("MaxDist: " + maxDist + ", CurrentDist: "
	// // + minDistToB);
	// if (minDistToB <= maxDist) {
	// mo.setOthersSupport(true);
	// }
	// }
	// }
	//
	// public HashSet<String> boundarySegmentGenerator(float[] boundary) {
	// HashSet<String> boundarySegments = new HashSet<String>();
	// for (int i = 0; i < num_dims; i++) {
	// HashSet<String> previousList = new HashSet<String>();
	// HashSet<String> newList = new HashSet<String>();
	// for (int j = 0; j < num_dims; j++) {
	// previousList.addAll(newList);
	// newList.clear();
	// if (i == j) {
	// // add a segment
	// if (previousList.size() == 0) {
	// if (boundary[2 * j + 1] - boundary[2 * j] <= segmentLength)
	// newList.add(boundary[2 * j] + "," + boundary[2 * j + 1] + ",");
	// else {
	// float startValue = boundary[2 * j];
	// while (startValue + segmentLength < boundary[2 * j + 1]) {
	// newList.add(startValue + "," + (startValue + segmentLength) + ",");
	// startValue += segmentLength;
	// }
	// newList.add(startValue + "," + boundary[2 * j + 1] + ",");
	// }
	// } else {
	// for (String previousStr : previousList) {
	// if (boundary[2 * j + 1] - boundary[2 * j] <= segmentLength)
	// newList.add(previousStr + boundary[2 * j] + "," + boundary[2 * j + 1] +
	// ",");
	// else {
	// float startValue = boundary[2 * j];
	// while (startValue + segmentLength < boundary[2 * j + 1]) {
	// newList.add(
	// previousStr + startValue + "," + (startValue + segmentLength) + ",");
	// startValue += segmentLength;
	// }
	// newList.add(previousStr + startValue + "," + boundary[2 * j + 1] + ",");
	// }
	// }
	// }
	// } else {
	// // add two points
	// if (previousList.size() == 0) {
	// newList.add(boundary[2 * j] + "," + boundary[2 * j] + ",");
	// newList.add(boundary[2 * j + 1] + "," + boundary[2 * j + 1] + ",");
	// } else {
	// for (String previousStr : previousList) {
	// newList.add(previousStr + boundary[2 * j] + "," + boundary[2 * j] + ",");
	// newList.add(previousStr + boundary[2 * j + 1] + "," + boundary[2 * j + 1]
	// + ",");
	// }
	// }
	// }
	// previousList.clear();
	// }
	// boundarySegments.addAll(newList);
	// }
	// return boundarySegments;
	// }
	//
	// public float[] StringToFloatArray(String str) {
	// String[] splitStr = str.split(",");
	// float[] array = new float[splitStr.length];
	// for (int i = 0; i < splitStr.length; i++)
	// array[i] = Float.parseFloat(splitStr[i]);
	// return array;
	// }
	//
	// public void computeSafePruningPoints(ArrayList<MetricObject>
	// listOfPoints,
	// HashMap<Integer, float[]> partition_store, int currentPid) {
	// float[] currentPartition = partition_store.get(currentPid);
	//// for (Map.Entry<Integer, float[]> adjPartition :
	// partition_store.entrySet()) {
	//// if (adjPartition.getKey() == currentPid)
	//// continue;
	//// // first compute boundary between two partitions
	//// float[] boundary = boundaryOfTwoPartitions(currentPartition,
	// adjPartition.getValue());
	//// // compute knn of the boundary
	//// float maxDist = computeKNNForBoundary(boundary, listOfPoints);
	//// // filter points
	//// filterDataPoints(boundary, listOfPoints, maxDist);
	//// }
	//
	// // divide the boundary into several boundary segments
	// HashSet<String> boundaryList =
	// boundarySegmentGenerator(currentPartition);
	// for (String s : boundaryList) {
	// // System.out.println("Segments: " + s);
	// // compute knn of the boundary
	// float maxDist = computeKNNForBoundary(StringToFloatArray(s),
	// listOfPoints);
	// // System.out.println("MaxDist: " + maxDist);
	// // filter points
	// filterDataPoints(StringToFloatArray(s), listOfPoints, maxDist);
	// }
	// }
	//
	// /**
	// * default Reduce class.
	// *
	// * @author Yizhou Yan
	// * @version Aug 24, 2016
	// * @throws InterruptedException
	// */
	// public void reduce(IntWritable key, Iterable<Text> values, Context
	// context)
	// throws IOException, InterruptedException {
	// // current partition id
	// // int currentPid = key.pid;
	// int currentPid = key.get();
	// // partition information for current partition and adjacent
	// // partition
	// HashMap<Integer, float[]> partition_store = new HashMap<Integer,
	// float[]>();
	//
	// ArrayList<MetricObject> pointList = new ArrayList<MetricObject>();
	// for (Text value : values) {
	// if (value.toString().contains("PInfo")) {
	// // add to partition information
	// String[] splitStr =
	// value.toString().split(":")[1].split(SQConfig.sepStrForRecord);
	// int tempPID = Integer.parseInt(splitStr[0]);
	// if (partition_store.containsKey(tempPID)) {
	// // update partition_store
	// float[] tempPartition = partition_store.get(tempPID);
	// for (int i = 0; i < num_dims; i++) {
	// tempPartition[2 * i] = Math.min(tempPartition[2 * i],
	// Float.parseFloat(splitStr[2 * i + 1]));
	// tempPartition[2 * i + 1] = Math.max(tempPartition[2 * i + 1],
	// Float.parseFloat(splitStr[2 * i + 2]));
	// }
	// partition_store.put(tempPID, tempPartition);
	// } else {
	// // add to partition_store
	// float[] tempPartition = new float[num_dims * 2];
	// for (int i = 0; i < num_dims; i++) {
	// tempPartition[2 * i] = Float.parseFloat(splitStr[2 * i + 1]);
	// tempPartition[2 * i + 1] = Float.parseFloat(splitStr[2 * i + 2]);
	// }
	// partition_store.put(tempPID, tempPartition);
	// }
	//
	// } else {
	// // MetricObject mo = parseObject(key.pid, value.toString());
	// MetricObject mo = parseObject(key.get(), value.toString());
	// pointList.add(mo);
	// }
	// } // end collect data
	//
	// // for(Map.Entry<Integer, float[]> e: partition_store.entrySet()){
	// // System.out.println(e.getKey() + "," + e.getValue()[4] + "," +
	// // e.getValue()[5] + "," +
	// // e.getValue()[6] + "," + e.getValue()[7]);
	// // }
	// // compute a list of objects that are other partitions' support
	// // points
	// computeSafePruningPoints(pointList, partition_store, currentPid);
	// int countOutputPoint = 0;
	// for (MetricObject mo : pointList) {
	// if (mo.isOthersSupport())
	// countOutputPoint++;
	// }
	// context.getCounter(Counters.TotalPoints).increment(pointList.size());
	// context.getCounter(Counters.NeedOutputPoints).increment(countOutputPoint);
	//
	// System.out.println("Total Points in this partition: " + pointList.size()
	// + " Total output points: "
	// + countOutputPoint);
	// // if (pointList.size() < K + 1)
	// // System.out.println("Less points than K + 1");
	// // else {
	// //
	// // ArrayList<LargeCellStore> leaveNodes = new
	// // ArrayList<LargeCellStore>();
	// // SafeArea sa = new SafeArea(partition_store);
	// // partitionTreeNode ptn = ClosestPair.divideAndConquer(pointList,
	// // partition_store, leaveNodes,
	// // sa, K, metric, metricSpace);
	// //
	// // float[] safeAbsArea = sa.getExtendAbsSize();
	// // float[] safeArea = {
	// // Math.min(partition_store[0] + safeAbsArea[0],
	// // partition_store[1]),
	// // Math.max(partition_store[0], partition_store[1] -
	// // safeAbsArea[1]),
	// // Math.min(partition_store[2] + safeAbsArea[2],
	// // partition_store[3]),
	// // Math.max(partition_store[3] - safeAbsArea[3], partition_store[2])
	// // };
	// // for (int i = 0; i < leaveNodes.size(); i++) {
	// // if (areaInsideSafeArea(safeArea,
	// // leaveNodes.get(i).getCoordinates())) {
	// // leaveNodes.get(i).setSafeArea(true);
	// // }
	// // }
	// // // sort by priority
	// // Collections.sort(leaveNodes, new Comparator<LargeCellStore>() {
	// // public int compare(LargeCellStore l1, LargeCellStore l2) {
	// // if (l1.getBucketPriority() > l2.getBucketPriority())
	// // return -1;
	// // else if (l1.getBucketPriority() == l2.getBucketPriority())
	// // return 0;
	// // else
	// // return 1;
	// // }
	// // });
	// //
	// // // save points that can prune
	// // HashMap<Long, MetricObject> CanPrunePoints = new HashMap<Long,
	// // MetricObject>();
	// //
	// // // save points that can find exact knns
	// // HashMap<Long, MetricObject> TrueKnnPoints = new HashMap<Long,
	// // MetricObject>();
	// // // start calculating LRD and LOF if possible
	// //
	// // HashMap<Long, MetricObject> lrdHM = new HashMap<Long,
	// // MetricObject>();
	// // for (int i = 0; i < leaveNodes.size(); i++) {
	// // HashMap<Long, MetricObject> TempCanPrunePoints = new
	// // HashMap<Long, MetricObject>();
	// // HashMap<Long, MetricObject> TempTrueKnnPoints = new HashMap<Long,
	// // MetricObject>();
	// // // start calculating LRD and LOF if possible
	// // // save those pruned points but need to recompute KNNs
	// // HashMap<Long, MetricObject> TempneedCalculatePruned = new
	// // HashMap<Long, MetricObject>();
	// // HashMap<Long, MetricObject> TemplrdHM = new HashMap<Long,
	// // MetricObject>();
	// // // save those cannot be pruned only by LRD value...
	// // HashMap<Long, MetricObject> TempneedCalLOF = new HashMap<Long,
	// // MetricObject>();
	// // // need more knn information, maybe knn is pruned...
	// // HashMap<Long, MetricObject> TempneedRecalLRD = new HashMap<Long,
	// // MetricObject>();
	// //
	// // // build index for the LargeCellStore
	// // if (leaveNodes.get(i).getNumOfPoints() == 0) {
	// // continue;
	// // } else if (leaveNodes.get(i).getNumOfPoints() > 5 * K) {
	// // leaveNodes.get(i).seperateToSmallCells(TempCanPrunePoints, i,
	// // thresholdLof, K, safeArea, ptn,
	// // partition_store);
	// // if (!leaveNodes.get(i).isBreakIntoSmallCells() &&
	// // leaveNodes.get(i).getNumOfPoints() > K * 20) {
	// // leaveNodes.get(i).seperateLargeNoPrune(K, i, safeArea);
	// // }
	// // }
	// // CanPrunePoints.putAll(TempCanPrunePoints);
	// //
	// // // Inner bucket KNN search
	// // if (leaveNodes.get(i).isBreakIntoSmallCells()) {
	// // leaveNodes.get(i).findKnnsWithinPRTreeInsideBucket(TempTrueKnnPoints,
	// // leaveNodes, i, K);
	// // } else if (leaveNodes.get(i).getNumOfPoints() != 0) {
	// // // else find kNNs within the large cell
	// // leaveNodes.get(i).findKnnsForLargeCellInsideBucket(TempTrueKnnPoints,
	// // leaveNodes, i, K);
	// // }
	// //
	// // // Inner bucket LRD computation
	// // for (MetricObject mo : TempTrueKnnPoints.values()) {
	// // CalLRDForSingleObject(mo, TempTrueKnnPoints, TempCanPrunePoints,
	// // TempneedCalculatePruned,
	// // TemplrdHM, TempneedCalLOF, TempneedRecalLRD, thresholdLof,
	// // context, leaveNodes);
	// // }
	// //
	// // // for those pruned by cell-based pruning, find kNNs for
	// // // these
	// // // points
	// // for (MetricObject mo : TempneedCalculatePruned.values()) {
	// // prQuadLeaf curLeaf =
	// // leaveNodes.get(i).findLeafWithSmallCellIndex(
	// // leaveNodes.get(i).getRootForPRTree(),
	// // mo.getIndexForSmallCell()[0],
	// // mo.getIndexForSmallCell()[1]);
	// // leaveNodes.get(i).findKnnsForOnePointInsideBucket(TempTrueKnnPoints,
	// // mo, curLeaf,
	// // leaveNodes.get(i), K);
	// //
	// // }
	// //
	// // // knn's knn is pruned...
	// // HashMap<Long, MetricObject> TempneedCalculateLRDPruned = new
	// // HashMap<Long, MetricObject>();
	// // // calculate LRD for some points again....
	// // for (MetricObject mo : TempneedRecalLRD.values()) {
	// // ReCalLRDForSpecial(context, mo, TempTrueKnnPoints,
	// // TempneedCalculatePruned, TemplrdHM,
	// // TempneedCalLOF, TempneedCalculateLRDPruned, thresholdLof);
	// // }
	// //
	// // // (needs implementation) calculate LRD for points that
	// // // needs
	// // // Calculate LRD (deal with needCalculateLRDPruned)
	// // for (MetricObject mo : TempneedCalculateLRDPruned.values()) {
	// // float lrd_core = 0.0f;
	// // boolean canCalLRD = true;
	// // long[] KNN_moObjectsID = mo.getPointPQ().getValueSet();
	// // float[] moDistToKNN = mo.getPointPQ().getPrioritySet();
	// //
	// // for (int j = 0; j < KNN_moObjectsID.length; j++) {
	// // long knn_mo = KNN_moObjectsID[j];
	// // // first point out which large cell it is in
	// // // (tempIndexX, tempIndexY)
	// // float kdistknn = 0.0f;
	// // if (TempTrueKnnPoints.containsKey(knn_mo))
	// // kdistknn = TempTrueKnnPoints.get(knn_mo).getKdist();
	// // else if (TempCanPrunePoints.containsKey(knn_mo)
	// // && (!TempneedCalculatePruned.containsKey(knn_mo))) {
	// // MetricObject newKnnFind = TempCanPrunePoints.get(knn_mo);
	// // prQuadLeaf curLeaf =
	// // leaveNodes.get(i).findLeafWithSmallCellIndex(
	// // leaveNodes.get(i).getRootForPRTree(),
	// // newKnnFind.getIndexForSmallCell()[0],
	// // newKnnFind.getIndexForSmallCell()[1]);
	// // leaveNodes.get(i).findKnnsForOnePointInsideBucket(TempTrueKnnPoints,
	// // newKnnFind,
	// // curLeaf, leaveNodes.get(i), K);
	// // if (TempTrueKnnPoints.containsKey(knn_mo))
	// // kdistknn = TempTrueKnnPoints.get(knn_mo).getKdist();
	// // else {
	// // canCalLRD = false;
	// // break;
	// // }
	// // } else {
	// // canCalLRD = false;
	// // break;
	// // }
	// // float temp_reach_dist = Math.max(moDistToKNN[j], kdistknn);
	// // lrd_core += temp_reach_dist;
	// // // System.out.println("Found KNNs for pruning point:
	// // // " +
	// // // mo.getKdist());
	// // }
	// // if (canCalLRD) {
	// // lrd_core = 1.0f / (lrd_core / K * 1.0f);
	// // mo.setLrdValue(lrd_core);
	// // mo.setType('L');
	// // TemplrdHM.put(((Record) mo.getObj()).getRId(), mo);
	// // }
	// // }
	// // // Inner bucket LOF computation
	// // for (MetricObject mo : TempneedCalLOF.values()) {
	// // CalLOFForSingleObject(context, mo, TemplrdHM);
	// // if (mo.getType() == 'O' && mo.getLofValue() > thresholdLof) {
	// // float tempLofValue = mo.getLofValue();
	// // if (topnLOF.size() < topNNumber) {
	// // topnLOF.insert(metricSpace.getID(mo.getObj()), tempLofValue);
	// //
	// // } else if (tempLofValue > topnLOF.getPriority()) {
	// // topnLOF.pop();
	// // topnLOF.insert(metricSpace.getID(mo.getObj()), tempLofValue);
	// // if (thresholdLof < topnLOF.getPriority())
	// // thresholdLof = topnLOF.getPriority();
	// // // System.out.println("Threshold updated: " +
	// // // thresholdLof);
	// // }
	// // }
	// // }
	// // if (topnLOF.size() == topNNumber && thresholdLof <
	// // topnLOF.getPriority())
	// // thresholdLof = topnLOF.getPriority();
	// // TrueKnnPoints.putAll(TempTrueKnnPoints);
	// // lrdHM.putAll(TemplrdHM);
	// // }
	// //
	// //
	// context.getCounter(Counters.CellPrunedPoints).increment(CanPrunePoints.size());
	// //
	// // String outputKDistPath =
	// // context.getConfiguration().get(SQConfig.strKdistanceOutput);
	// // String outputPPPath =
	// // context.getConfiguration().get(SQConfig.strKnnPartitionPlan);
	// //
	// // /**
	// // * deal with boundary points also deal with large cells that not
	// // * broke up
	// // *
	// // * bound supporting area for the partition
	// // */
	// // float[] partitionExpand = { 0.0f, 0.0f, 0.0f, 0.0f };
	// // for (int i = 0; i < leaveNodes.size(); i++) {
	// // if (leaveNodes.get(i).isBreakIntoSmallCells()) {
	// // // find kNNs within the PR quad tree
	// // partitionExpand = maxOfTwoFloatArray(partitionExpand,
	// // leaveNodes.get(i).findKnnsWithinPRTreeOutsideBucket(TrueKnnPoints,
	// // leaveNodes, i, ptn,
	// // partition_store, K, num_dims, domains));
	// //
	// // } else if (leaveNodes.get(i).getNumOfPoints() != 0) {
	// // // else find kNNs within the large cell
	// // partitionExpand = maxOfTwoFloatArray(partitionExpand,
	// // leaveNodes.get(i).findKnnsForLargeCellOutsideBucket(TrueKnnPoints,
	// // leaveNodes, i, ptn,
	// // partition_store, K, num_dims, domains));
	// // }
	// // }
	// // ////
	// //
	// context.getCounter(Counters.CanCalKNNs).increment(TrueKnnPoints.size());
	// //
	// // // save those pruned points but need to recompute KNNs
	// // HashMap<Long, MetricObject> needCalculatePruned = new
	// // HashMap<Long, MetricObject>();
	// // // save those cannot be pruned only by LRD value...
	// // HashMap<Long, MetricObject> needCalLOF = new HashMap<Long,
	// // MetricObject>();
	// // // need more knn information, maybe knn is pruned...
	// // HashMap<Long, MetricObject> needRecalLRD = new HashMap<Long,
	// // MetricObject>();
	// //
	// // // calculate LRD for points that can calculate
	// // for (MetricObject mo : TrueKnnPoints.values()) {
	// // if (mo.getType() == 'T')
	// // CalLRDForSingleObject(mo, TrueKnnPoints, CanPrunePoints,
	// // needCalculatePruned, lrdHM, needCalLOF,
	// // needRecalLRD, thresholdLof, context, leaveNodes);
	// // }
	// //
	// // // for those pruned by cell-based pruning, find kNNs for these
	// // // points
	// // for (MetricObject mo : needCalculatePruned.values()) {
	// // int tempIndex = mo.getIndexOfCPCellInList();
	// // prQuadLeaf curLeaf =
	// // leaveNodes.get(tempIndex).findLeafWithSmallCellIndex(
	// // leaveNodes.get(tempIndex).getRootForPRTree(),
	// // mo.getIndexForSmallCell()[0],
	// // mo.getIndexForSmallCell()[1]);
	// // if (!mo.isInsideKNNfind()) {
	// //
	// leaveNodes.get(tempIndex).findKnnsForOnePointInsideBucket(TrueKnnPoints,
	// // mo, curLeaf,
	// // leaveNodes.get(tempIndex), K);
	// // }
	// //
	// leaveNodes.get(tempIndex).findKnnsForOnePointOutsideBucket(TrueKnnPoints,
	// // mo, curLeaf, leaveNodes,
	// // leaveNodes.get(tempIndex), ptn, partition_store, K, num_dims,
	// // domains);
	// //
	// // }
	// // // knn's knn is pruned...
	// // HashMap<Long, MetricObject> needCalculateLRDPruned = new
	// // HashMap<Long, MetricObject>();
	// // // calculate LRD for some points again....
	// // for (MetricObject mo : needRecalLRD.values()) {
	// // ReCalLRDForSpecial(context, mo, TrueKnnPoints,
	// // needCalculatePruned, lrdHM, needCalLOF,
	// // needCalculateLRDPruned, thresholdLof);
	// // }
	// //
	// // // (needs implementation) calculate LRD for points that needs
	// // // Calculate LRD (deal with needCalculateLRDPruned)
	// // for (MetricObject mo : needCalculateLRDPruned.values()) {
	// // float lrd_core = 0.0f;
	// // boolean canCalLRD = true;
	// // long[] KNN_moObjectsID = mo.getPointPQ().getValueSet();
	// // float[] moDistToKNN = mo.getPointPQ().getPrioritySet();
	// //
	// // for (int i = 0; i < KNN_moObjectsID.length; i++) {
	// // long knn_mo = KNN_moObjectsID[i];
	// // // first point out which large cell it is in
	// // // (tempIndexX, tempIndexY)
	// // float kdistknn = 0.0f;
	// // if (TrueKnnPoints.containsKey(knn_mo))
	// // kdistknn = TrueKnnPoints.get(knn_mo).getKdist();
	// // else if (CanPrunePoints.containsKey(knn_mo) &&
	// // (!needCalculatePruned.containsKey(knn_mo))) {
	// // MetricObject newKnnFind = CanPrunePoints.get(knn_mo);
	// // int tempIndex = newKnnFind.getIndexOfCPCellInList();
	// // prQuadLeaf curLeaf =
	// // leaveNodes.get(tempIndex).findLeafWithSmallCellIndex(
	// // leaveNodes.get(tempIndex).getRootForPRTree(),
	// // newKnnFind.getIndexForSmallCell()[0],
	// // newKnnFind.getIndexForSmallCell()[1]);
	// // if (!newKnnFind.isInsideKNNfind()) {
	// //
	// leaveNodes.get(tempIndex).findKnnsForOnePointInsideBucket(TrueKnnPoints,
	// // newKnnFind,
	// // curLeaf, leaveNodes.get(tempIndex), K);
	// // }
	// //
	// leaveNodes.get(tempIndex).findKnnsForOnePointOutsideBucket(TrueKnnPoints,
	// // newKnnFind,
	// // curLeaf, leaveNodes, leaveNodes.get(tempIndex), ptn,
	// // partition_store, K,
	// // num_dims, domains);
	// // if (TrueKnnPoints.containsKey(knn_mo))
	// // kdistknn = TrueKnnPoints.get(knn_mo).getKdist();
	// // else {
	// // canCalLRD = false;
	// // break;
	// // }
	// // } else {
	// // canCalLRD = false;
	// // break;
	// // }
	// // float temp_reach_dist = Math.max(moDistToKNN[i], kdistknn);
	// // lrd_core += temp_reach_dist;
	// // // System.out.println("Found KNNs for pruning point: " +
	// // // mo.getKdist());
	// // }
	// // if (canCalLRD) {
	// // lrd_core = 1.0f / (lrd_core / K * 1.0f);
	// // mo.setLrdValue(lrd_core);
	// // mo.setType('L');
	// // lrdHM.put(((Record) mo.getObj()).getRId(), mo);
	// // }
	// // } // end calculate LRD for pruned points
	// //
	// ////////////////////////////////////////////////////////////////////////////////////////
	// // //
	// //
	// context.getCounter(Counters.PruneCalLRD).increment(needCalculateLRDPruned.size());
	// // //
	// //
	// context.getCounter(Counters.UsedPrune).increment(needCalculatePruned.size());
	// // //
	// // context.getCounter(Counters.CanCalLRDs).increment(lrdHM.size());
	// // //
	// // context.getCounter(Counters.NeedLOFCal).increment(needCalLOF.size());
	// //
	// // // calculate LOF for points that can calculate
	// // for (MetricObject mo : needCalLOF.values()) {
	// // CalLOFForSingleObject(context, mo, lrdHM);
	// // if (mo.getType() == 'O' && mo.getLofValue() > thresholdLof) {
	// // float tempLofValue = mo.getLofValue();
	// // if (topnLOF.size() < topNNumber) {
	// // topnLOF.insert(metricSpace.getID(mo.getObj()), tempLofValue);
	// //
	// // } else if (tempLofValue > topnLOF.getPriority()) {
	// // topnLOF.pop();
	// // topnLOF.insert(metricSpace.getID(mo.getObj()), tempLofValue);
	// // if (thresholdLof < topnLOF.getPriority())
	// // thresholdLof = topnLOF.getPriority();
	// // // System.out.println("Threshold updated: " +
	// // // thresholdLof);
	// // }
	// // }
	// // }
	// // if (topnLOF.size() == topNNumber && thresholdLof <
	// // topnLOF.getPriority())
	// // thresholdLof = topnLOF.getPriority();
	// // // output data points and calculate partition bound
	// //
	// // for (int i = 0; i < leaveNodes.size(); i++) {
	// // if (leaveNodes.get(i).isSafeArea()) {
	// // for (MetricObject o_R : leaveNodes.get(i).getListOfPoints()) {
	// // if (!o_R.isCanPrune() && o_R.getType() != 'O')
	// // outputMultipleTypeData(o_R, outputKDistPath, lrdHM,
	// // needCalculatePruned, context);
	// // }
	// // } else {
	// // for (MetricObject o_R : leaveNodes.get(i).getListOfPoints()) {
	// // if (pointInsideSafeArea(safeArea, ((Record)
	// // o_R.getObj()).getValue())
	// // && (o_R.isCanPrune() || o_R.getType() == 'O'))
	// // continue;
	// // else
	// // outputMultipleTypeData(o_R, outputKDistPath, lrdHM,
	// // needCalculatePruned, context);
	// // }
	// // }
	// // }
	// //
	// // // output partition plan
	// // mos.write(new LongWritable(1),
	// // new Text(currentPid + SQConfig.sepStrForRecord +
	// // partition_store[0]
	// // + SQConfig.sepStrForRecord + partition_store[1] +
	// // SQConfig.sepStrForRecord
	// // + partition_store[2] + SQConfig.sepStrForRecord
	// // + partition_store[3] + SQConfig.sepStrForRecord +
	// // partitionExpand[0]
	// // + SQConfig.sepStrForRecord + partitionExpand[1] +
	// // SQConfig.sepStrForRecord
	// // + partitionExpand[2] + SQConfig.sepStrForRecord +
	// // partitionExpand[3]),
	// // outputPPPath + "/pp_" + context.getTaskAttemptID());
	// // System.err.println("computation finished");
	// // }
	// } // end
	// // reduce
	// // function
	//
	// public boolean pointInsideSafeArea(float[] safeArea, float[] coordinates)
	// {
	// if (coordinates[0] >= safeArea[0] && coordinates[0] <= safeArea[1] &&
	// coordinates[1] >= safeArea[2]
	// && coordinates[1] <= safeArea[3])
	// return true;
	// else
	// return false;
	// }
	//
	// public boolean areaInsideSafeArea(float[] safeArea, float[] extendedArea)
	// {
	// if (extendedArea[0] >= safeArea[0] && extendedArea[1] <= safeArea[1] &&
	// extendedArea[2] >= safeArea[2]
	// && extendedArea[3] <= safeArea[3])
	// return true;
	// else
	// return false;
	// }
	//
	// /**
	// * Calculate LRD if possible
	// *
	// * @param context
	// * @param o_S
	// * @param TrueKnnPoints
	// * @param CanPrunePoints
	// * @param needCalculatePruned
	// * @param lrdHM
	// * @param needCalLOF
	// * @param threshold
	// * @return
	// * @throws IOException
	// * @throws InterruptedException
	// */
	// private boolean CalLRDForSingleObject(MetricObject o_S, HashMap<Long,
	// MetricObject> TrueKnnPoints,
	// HashMap<Long, MetricObject> CanPrunePoints, HashMap<Long, MetricObject>
	// needCalculatePruned,
	// HashMap<Long, MetricObject> lrdHM, HashMap<Long, MetricObject>
	// needCalLOF,
	// HashMap<Long, MetricObject> needRecalLRD, float threshold, Context
	// context,
	// ArrayList<LargeCellStore> leaveNodes) throws IOException,
	// InterruptedException {
	// float lrd_core = 0.0f;
	// float reachdistMax = 0.0f;
	// float minNNtoNN = Float.POSITIVE_INFINITY;
	// // boolean canLRD = true;
	// int countPruned = 0;
	// HashMap<Long, MetricObject> tempNeedCalculatePruned = new HashMap<Long,
	// MetricObject>();
	//
	// long[] KNN_moObjectsID = o_S.getPointPQ().getValueSet();
	// float[] moDistToKNN = o_S.getPointPQ().getPrioritySet();
	// for (int i = 0; i < moDistToKNN.length; i++) {
	// long temp_kNNKey = KNN_moObjectsID[i];
	// float temp_dist = moDistToKNN[i];
	// if (!TrueKnnPoints.containsKey(temp_kNNKey)) {
	// if (CanPrunePoints.containsKey(temp_kNNKey)) {
	// tempNeedCalculatePruned.put(temp_kNNKey,
	// CanPrunePoints.get(temp_kNNKey));
	// reachdistMax = Math.max(temp_dist, reachdistMax);
	// minNNtoNN = Math.min(minNNtoNN,
	// leaveNodes.get(CanPrunePoints.get(temp_kNNKey).getIndexOfCPCellInList()).getCpDist());
	// continue;
	// } else
	// return false;
	// }
	// float temp_reach_dist = Math.max(temp_dist,
	// TrueKnnPoints.get(temp_kNNKey).getKdist());
	// reachdistMax = Math.max(reachdistMax, temp_reach_dist);
	// minNNtoNN = Math.min(minNNtoNN,
	// TrueKnnPoints.get(temp_kNNKey).getNearestNeighborDist());
	// lrd_core += temp_reach_dist;
	// }
	// // if (!canLRD) {
	// // needRecalLRD.put(((Record) o_S.getObj()).getRId(), o_S);
	// // return false;
	// // }
	// if (tempNeedCalculatePruned.isEmpty()) {
	// lrd_core = 1.0f / (lrd_core / K * 1.0f);
	// o_S.setLrdValue(lrd_core);
	// o_S.setType('L');
	// // calculate if this can prune? if can prune, then don't
	// // calculate
	// // lof
	// float predictedLOF = reachdistMax / minNNtoNN;
	// lrdHM.put(((Record) o_S.getObj()).getRId(), o_S);
	// if (predictedLOF <= threshold) {
	// o_S.setCanPrune(true);
	// countPruned++;
	// } else
	// needCalLOF.put(((Record) o_S.getObj()).getRId(), o_S);
	// // System.out.println("LRD-----" + lrd_core);
	// context.getCounter(Counters.LRDPrunedPoints).increment(countPruned);
	// return true;
	// } else {
	// if (reachdistMax / minNNtoNN <= threshold) {
	// o_S.setCanPrune(true);
	// countPruned++;
	// context.getCounter(Counters.LRDPrunedPoints).increment(countPruned);
	// return true;
	// } else {
	// needCalculatePruned.putAll(tempNeedCalculatePruned);
	// needRecalLRD.put(((Record) o_S.getObj()).getRId(), o_S);
	// return false;
	// }
	// }
	// }
	//
	// private void CalLOFForSingleObject(Context context, MetricObject o_S,
	// HashMap<Long, MetricObject> lrdHm)
	// throws IOException, InterruptedException {
	// float lof_core = 0.0f;
	// int countPruned = 0;
	// if (o_S.getLrdValue() == 0)
	// lof_core = 0;
	// else {
	// long[] KNN_moObjectsID = o_S.getPointPQ().getValueSet();
	//
	// for (int i = 0; i < KNN_moObjectsID.length; i++) {
	// long temp_kNNKey = KNN_moObjectsID[i];
	//
	// if (!lrdHm.containsKey(temp_kNNKey)) {
	// return;
	// }
	// float temp_lrd = lrdHm.get(temp_kNNKey).getLrdValue();
	// if (temp_lrd == 0 || o_S.getLrdValue() == 0)
	// continue;
	// else
	// lof_core += temp_lrd / o_S.getLrdValue() * 1.0f;
	// }
	// lof_core = lof_core / K * 1.0f;
	// }
	// if (Float.isNaN(lof_core) || Float.isInfinite(lof_core))
	// lof_core = 0;
	// o_S.setLofValue(lof_core);
	// o_S.setType('O'); // calculated LOF
	// // context.getCounter(Counters.CanCalLOFs).increment(1);
	// if (lof_core <= thresholdLof) {
	// // context.getCounter(Counters.ThirdPrune).increment(1);
	// countPruned++;
	// o_S.setCanPrune(true);
	// }
	// context.getCounter(Counters.FinalPrunedPoints).increment(countPruned);
	// }
	//
	// /**
	// * Calculate LRD for some points that knns pruned
	// *
	// * @param context
	// * @param o_S
	// * @param TrueKnnPoints
	// * @param needCalculatePruned
	// * @param lrdHM
	// * @param needCalLOF
	// * @param threshold
	// * @return
	// * @throws IOException
	// * @throws InterruptedException
	// */
	// private boolean ReCalLRDForSpecial(Context context, MetricObject o_S,
	// HashMap<Long, MetricObject> TrueKnnPoints,
	// HashMap<Long, MetricObject> needCalculatePruned, HashMap<Long,
	// MetricObject> lrdHM,
	// HashMap<Long, MetricObject> needCalLOF, HashMap<Long, MetricObject>
	// needCalculateLRDPruned,
	// float threshold) throws IOException, InterruptedException {
	// float lrd_core = 0.0f;
	// float reachdistMax = 0.0f;
	// float minNNtoNN = Float.POSITIVE_INFINITY;
	// int countPruned = 0;
	//
	// long[] KNN_moObjectsID = o_S.getPointPQ().getValueSet();
	// float[] moDistToKNN = o_S.getPointPQ().getPrioritySet();
	// for (int i = 0; i < KNN_moObjectsID.length; i++) {
	// long temp_kNNKey = KNN_moObjectsID[i];
	// float temp_dist = moDistToKNN[i];
	// float temp_reach_dist = 0.0f;
	// if (!TrueKnnPoints.containsKey(temp_kNNKey)) {
	// if (needCalculatePruned.containsKey(temp_kNNKey)) {
	// temp_reach_dist = Math.max(temp_dist,
	// needCalculatePruned.get(temp_kNNKey).getKdist());
	// reachdistMax = Math.max(reachdistMax, temp_reach_dist);
	// minNNtoNN = Math.min(minNNtoNN,
	// needCalculatePruned.get(temp_kNNKey).getNearestNeighborDist());
	// }
	// } else {
	// temp_reach_dist = Math.max(temp_dist,
	// TrueKnnPoints.get(temp_kNNKey).getKdist());
	// reachdistMax = Math.max(reachdistMax, temp_reach_dist);
	// minNNtoNN = Math.min(minNNtoNN,
	// TrueKnnPoints.get(temp_kNNKey).getNearestNeighborDist());
	// }
	// lrd_core += temp_reach_dist;
	// }
	// lrd_core = 1.0f / (lrd_core / K * 1.0f);
	// o_S.setLrdValue(lrd_core);
	// o_S.setType('L');
	// // calculate if this can prune? if can prune, then don't calculate
	// // lof
	// float predictedLOF = reachdistMax / minNNtoNN;
	// lrdHM.put(((Record) o_S.getObj()).getRId(), o_S);
	// if (predictedLOF <= threshold) {
	// // context.getCounter(Counters.SecondPrune).increment(1);
	// o_S.setCanPrune(true);
	// countPruned++;
	// } else {
	// needCalLOF.put(((Record) o_S.getObj()).getRId(), o_S);
	// // long[] KNN_moObjectsID = o_S.getPointPQ().getValueSet();
	// for (int i = 0; i < KNN_moObjectsID.length; i++) {
	// long temp_kNNKey = KNN_moObjectsID[i];
	// if (needCalculatePruned.containsKey(temp_kNNKey))
	// needCalculateLRDPruned.put(((Record)
	// needCalculatePruned.get(temp_kNNKey).getObj()).getRId(),
	// needCalculatePruned.get(temp_kNNKey));
	// }
	// }
	// // System.out.println("LRD-----" + lrd_core);
	// context.getCounter(Counters.LRDPrunedPoints).increment(countPruned);
	// return true;
	// }
	//
	// /**
	// * output different types of data points in multiple files
	// *
	// * @param context
	// * @param o_R
	// * @throws InterruptedException
	// * @throws IOException
	// */
	// public void outputMultipleTypeData(MetricObject o_R, String
	// outputKDistPath, HashMap<Long, MetricObject> lrdHM,
	// HashMap<Long, MetricObject> needCalculatePruned, Context context)
	// throws IOException, InterruptedException {
	// // output format key:nid value: point value, partition id,canPrune,
	// // tag,
	// // (KNN's nid and dist|kdist|lrd for that point),k-distance, lrd,
	// // lof,
	// LongWritable outputKey = new LongWritable();
	// Text outputValue = new Text();
	//
	// String line = "";
	// line = line + o_R.getPartition_id() + SQConfig.sepStrForRecord;
	// if (o_R.isCanPrune()) {
	// line = line + "T";
	// line = line + SQConfig.sepStrForRecord + o_R.getType() +
	// SQConfig.sepStrForRecord;
	// line = line + o_R.getKdist() + SQConfig.sepStrForRecord +
	// o_R.getLrdValue() + SQConfig.sepStrForRecord
	// + o_R.getLofValue();
	// } else {
	// line = line + "F";
	// line = line + SQConfig.sepStrForRecord + o_R.getType() +
	// SQConfig.sepStrForRecord;
	// long[] KNN_moObjectsID = o_R.getPointPQ().getValueSet();
	// float[] moDistToKNN = o_R.getPointPQ().getPrioritySet();
	// for (int i = 0; i < moDistToKNN.length; i++) {
	// long keyMap = KNN_moObjectsID[i];
	// float valueMap = moDistToKNN[i];
	// float kdistForKNN = 0.0f;
	// float lrdForKNN = 0.0f;
	// if (lrdHM.containsKey(keyMap)) {
	// kdistForKNN = lrdHM.get(keyMap).getKdist();
	// lrdForKNN = lrdHM.get(keyMap).getLrdValue();
	// } else if (needCalculatePruned.containsKey(keyMap)) {
	// kdistForKNN = needCalculatePruned.get(keyMap).getKdist();
	// }
	// line = line + keyMap + SQConfig.sepStrForIDDist + valueMap +
	// SQConfig.sepStrForIDDist + kdistForKNN
	// + SQConfig.sepStrForIDDist + lrdForKNN + SQConfig.sepStrForRecord;
	// }
	// line = line + o_R.getKdist() + SQConfig.sepStrForRecord +
	// o_R.getLrdValue() + SQConfig.sepStrForRecord
	// + o_R.getLofValue();
	// }
	// outputKey.set(((Record) o_R.getObj()).getRId());
	// outputValue.set(((Record) o_R.getObj()).dimToString() +
	// SQConfig.sepStrForRecord + line);
	// mos.write(outputKey, outputValue, outputKDistPath + "/kdist_" +
	// context.getTaskAttemptID());
	// // context.write(outputKey, outputValue);
	// }
	//
	// public void cleanup(Context context) throws IOException,
	// InterruptedException {
	// // output top n LOF value
	// // String outputTOPNPath =
	// // context.getConfiguration().get(SQConfig.strKnnFirstRoundTopN);
	// // while (topnLOF.size() > 0) {
	// // mos.write(new LongWritable(topnLOF.getValue()), new
	// // Text(topnLOF.getPriority() + ""),
	// // outputTOPNPath + "/topn_" + context.getTaskAttemptID());
	// // topnLOF.pop();
	// // }
	// mos.close();
	// }
	//
	// }
	//
	// public void run(String[] args) throws Exception {
	// Configuration conf = new Configuration();
	// conf.addResource(new
	// Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
	// conf.addResource(new
	// Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
	// new GenericOptionsParser(conf, args).getRemainingArgs();
	// /** set job parameter */
	// Job job = Job.getInstance(conf, "Cell-Based KNN Searching: With Simple
	// Priority");
	//
	// job.setJarByClass(CalKdistanceFirstMultiDimSortByMax.class);
	// job.setMapperClass(FirstKNNFinderMapper.class);
	//
	// /** set multiple output path */
	// MultipleOutputs.addNamedOutput(job, "partitionplan",
	// TextOutputFormat.class, LongWritable.class, Text.class);
	// MultipleOutputs.addNamedOutput(job, "kdistance", TextOutputFormat.class,
	// LongWritable.class, Text.class);
	// MultipleOutputs.addNamedOutput(job, "topnLOF", TextOutputFormat.class,
	// LongWritable.class, Text.class);
	//
	// job.setReducerClass(FirstKNNFinderReducer.class);
	// // job.setMapOutputKeyClass(MetricKey.class);
	// job.setMapOutputKeyClass(IntWritable.class);
	// job.setMapOutputValueClass(Text.class);
	// job.setOutputKeyClass(LongWritable.class);
	// job.setOutputValueClass(Text.class);
	// job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
	// // job.setNumReduceTasks(0);
	// // job.setPartitionerClass(PriorityPartitioner.class);
	// // job.setSortComparatorClass(PriorityComparator.class);
	// // job.setGroupingComparatorClass(PriorityGroupComparator.class);
	//
	// String strFSName = conf.get("fs.default.name");
	// FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.dataset)));
	// FileSystem fs = FileSystem.get(conf);
	// fs.delete(new Path(conf.get(SQConfig.strKnnSummaryOutput)), true);
	// FileOutputFormat.setOutputPath(job, new
	// Path(conf.get(SQConfig.strKnnSummaryOutput)));
	// job.addCacheArchive(new URI(strFSName +
	// conf.get(SQConfig.strPartitionPlanOutput)));
	// job.addCacheArchive(new URI(strFSName +
	// conf.get(SQConfig.strCellsOutput)));
	// job.addCacheArchive(new URI(strFSName +
	// conf.get(SQConfig.strDimCorrelationOutput)));
	// job.addCacheArchive(new URI(strFSName +
	// conf.get(SQConfig.strPartitionAjacencyOutput)));
	// /** print job parameter */
	// System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression,
	// 10));
	// long begin = System.currentTimeMillis();
	// job.waitForCompletion(true);
	// long end = System.currentTimeMillis();
	// long second = (end - begin) / 1000;
	// System.err.println(job.getJobName() + " takes " + second + " seconds");
	// }
	//
	// public static void main(String[] args) {
	// CalKdistanceFirstMultiDimSortByMax findKnnAndSupporting = new
	// CalKdistanceFirstMultiDimSortByMax();
	// try {
	// findKnnAndSupporting.run(args);
	// } catch (Exception e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
}
