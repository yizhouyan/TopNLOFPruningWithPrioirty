package lof.pruning.firstknn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import lof.pruning.firstknn.CalKdistanceFirstMultiDim.Counters;
import metricspace.IMetric;
import metricspace.MetricObject;
import metricspace.Record;
import util.PriorityQueue;

public class SafePruningZone {
	private int num_dims;
	private int K;
	private float segmentLength;
	private IMetric metric;

	public SafePruningZone(int num_dims, float segmentLength, int k, IMetric metric) {
		this.num_dims = num_dims;
		this.segmentLength = segmentLength;
		this.K = k;
		this.metric = metric;
	}

	public static String floatArrayToString(float[] p) {
		String str = "";
		for (int i = 0; i < p.length; i++) {
			str += p[i] + ",";
		}
		str = str.substring(0, str.length() - 1);
		return str;
	}

	public HashSet<String> boundarySegmentGenerator(float[] boundary) {

		HashSet<String> boundarySegments = new HashSet<String>();
		for (int i = 0; i < num_dims; i++) {
			HashSet<String> previousList = new HashSet<String>();
			HashSet<String> newList = new HashSet<String>();
			for (int j = 0; j < num_dims; j++) {
				previousList.addAll(newList);
				newList.clear();
				if (i == j) {
					// add a segment
					if (previousList.size() == 0) {
						if (boundary[2 * j + 1] - boundary[2 * j] <= segmentLength)
							newList.add(boundary[2 * j] + "," + boundary[2 * j + 1] + ",");
						else {
							float startValue = boundary[2 * j];
							while (startValue + segmentLength < boundary[2 * j + 1]) {
								newList.add(startValue + "," + (startValue + segmentLength) + ",");
								startValue += segmentLength;
							}
							newList.add(startValue + "," + boundary[2 * j + 1] + ",");
						}
					} else {
						for (String previousStr : previousList) {
							if (boundary[2 * j + 1] - boundary[2 * j] <= segmentLength)
								newList.add(previousStr + boundary[2 * j] + "," + boundary[2 * j + 1] + ",");
							else {
								float startValue = boundary[2 * j];
								while (startValue + segmentLength < boundary[2 * j + 1]) {
									newList.add(previousStr + startValue + "," + (startValue + segmentLength) + ",");
									startValue += segmentLength;
								}
								newList.add(previousStr + startValue + "," + boundary[2 * j + 1] + ",");
							}
						}
					}
				} else {
					// add two points
					if (previousList.size() == 0) {
						newList.add(boundary[2 * j] + "," + boundary[2 * j] + ",");
						newList.add(boundary[2 * j + 1] + "," + boundary[2 * j + 1] + ",");
					} else {
						for (String previousStr : previousList) {
							newList.add(previousStr + boundary[2 * j] + "," + boundary[2 * j] + ",");
							newList.add(previousStr + boundary[2 * j + 1] + "," + boundary[2 * j + 1] + ",");
						}
					}
				}
				previousList.clear();
			}
			boundarySegments.addAll(newList);
		}
		return boundarySegments;
	}

	public float[] boundaryOfTwoPartitions(float[] p1, float[] p2) {
		float[] boundary = new float[p1.length];
		for (int i = 0; i < num_dims; i++) {
			boundary[2 * i] = Math.max(p1[2 * i], p2[2 * i]);
			boundary[2 * i + 1] = Math.min(p1[2 * i + 1], p2[2 * i + 1]);
		}
		return boundary;
	}

	public float[] StringToFloatArray(String str) {
		String[] splitStr = str.split(",");
		float[] array = new float[splitStr.length];
		for (int i = 0; i < splitStr.length; i++)
			array[i] = Float.parseFloat(splitStr[i]);
		return array;
	}

	public ArrayList<MetricObject> computeKNNForBoundary(float[] boundary, ArrayList<MetricObject> pointList) {
		PriorityQueue knnToBound = new PriorityQueue(PriorityQueue.SORT_ORDER_DESCENDING);
		for (int i = 0; i < pointList.size(); i++) {
			// compute min distance from point to boundary
			float tempMinDist = MinDistFromPointToBoundary(boundary, ((Record) pointList.get(i).getObj()).getValue());
			if (knnToBound.size() < K) {
				knnToBound.insert((long) i, tempMinDist);
			} else if (tempMinDist < knnToBound.getPriority()) {
				knnToBound.pop();
				knnToBound.insert(i, tempMinDist);
			}
		}
		ArrayList<MetricObject> knnToBoundList = new ArrayList<MetricObject>();
		for (long l : knnToBound.getValueSet()) {
			knnToBoundList.add(pointList.get((int) l));
		}
		return knnToBoundList;
	}

	public float MinDistFromPointToBoundary(float[] boundary, float[] pointCrds) {
		float[] newPoint = new float[pointCrds.length];
		float mindist = 0.0f;
		for (int i = 0; i < pointCrds.length; i++) {
			if (boundary[2 * i] >= pointCrds[i])
				newPoint[i] = boundary[2 * i];
			else if (boundary[2 * i + 1] <= pointCrds[i])
				newPoint[i] = boundary[2 * i + 1];
			else
				newPoint[i] = pointCrds[i];
		}
		try {
			mindist = metric.dist(new Record(0, pointCrds), new Record(0, newPoint));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mindist;
	}

	public void filterDataPoints(float[] boundary, ArrayList<MetricObject> listOfPoints, float maxDist) {
		for (MetricObject mo : listOfPoints) {
			float[] crds = ((Record) mo.getObj()).getValue();
			float minDistToB = MinDistFromPointToBoundary(boundary, crds);
			// System.out.println("MaxDist: " + maxDist + ", CurrentDist: "
			// + minDistToB + ",Current Point: " + floatArrayToString(crds)
			// + ", boundary:" + floatArrayToString(boundary));
			if (minDistToB <= maxDist) {
				mo.setOthersSupport(true);
			}
		}
	}

	public float MaxDistFromPointToBoundary(float[] boundary, float[] pointCrds) {
		float maxDist = 0.0f;
		float[] newPoint = new float[pointCrds.length];
		for (int i = 0; i < pointCrds.length; i++) {
			if (Math.abs(boundary[2 * i] - pointCrds[i]) > Math.abs(boundary[2 * i + 1] - pointCrds[i]))
				newPoint[i] = boundary[2 * i];
			else
				newPoint[i] = boundary[2 * i + 1];
		}

		try {
			maxDist = metric.dist(new Record(0, pointCrds), new Record(0, newPoint));
			// System.out.println("Max: " + maxDist);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return maxDist;
	}

	public float MaxDistFromPointListToBoundary(float[] boundary, ArrayList<MetricObject> knnToBoundList) {
		float maxDist = 0.0f;
		for (MetricObject mo : knnToBoundList) {
			maxDist = Math.max(maxDist, MaxDistFromPointToBoundary(boundary, ((Record) mo.getObj()).getValue()));

		}
		return maxDist;
	}

	public void computeSafePruningPoints(ArrayList<MetricObject> listOfPoints,
			HashMap<Integer, float[]> partition_store, int currentPid) {
		float[] currentPartition = partition_store.get(currentPid);
		for (Map.Entry<Integer, float[]> adjPartition : partition_store.entrySet()) {
			if (adjPartition.getKey() == currentPid)
				continue;
			// first compute boundary between two partitions
			float[] boundary = boundaryOfTwoPartitions(currentPartition, adjPartition.getValue());
			System.out.println("Boundary: " + floatArrayToString(boundary));
			// divide the boundary into several boundary segments
			HashSet<String> boundaryList = boundarySegmentGenerator(boundary);
			for (String s : boundaryList) {
				// System.out.println("Segments: " + s);
				// compute knn of the boundary
				ArrayList<MetricObject> knnToBoundList = computeKNNForBoundary(StringToFloatArray(s), listOfPoints);
				// compute maxdistance
				float maxDist = MaxDistFromPointListToBoundary(StringToFloatArray(s), knnToBoundList);
				// System.out.println("MaxDist: " + maxDist);
				// filter points
				filterDataPoints(StringToFloatArray(s), listOfPoints, maxDist);
			}
		}
	}

	public static void main(String[] args) {
		// usage
		// compute a list of objects that are other partitions' support
		// points
		// SafePruningZone comSafePruning = new SafePruningZone(4, 50, 3,
		// metric);
		// comSafePruning.computeSafePruningPoints(pointList, partition_store,
		// currentPid);
		// int countOutputPoint = 0;
		// for (MetricObject mo : pointList) {
		// if (mo.isOthersSupport())
		// countOutputPoint++;
		// }
		// context.getCounter(Counters.TotalPoints).increment(pointList.size());
		// context.getCounter(Counters.NeedOutputPoints).increment(countOutputPoint);
		//
		// System.out.println(
		// "Total Points in this partition: " + pointList.size() + " Total
		// output points: " + countOutputPoint);
	}
}
