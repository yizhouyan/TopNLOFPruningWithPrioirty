package lof.pruning.firstknn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Stack;

import lof.pruning.firstknn.CalKdistanceFirstMultiDim.Counters;
import lof.pruning.firstknn.prQuadTree.prQuadLeaf;
import metricspace.IMetric;
import metricspace.MetricObject;
import metricspace.Record;
import util.PriorityQueue;

public class SafePruningZoneOPT {
	private int num_dims;
	private int K;
	private float segmentLength;
	private IMetric metric;

	public SafePruningZoneOPT(int num_dims, float segmentLength, int k, IMetric metric) {
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

	static boolean checkRange(float[] expectedRange, float[] checkedRange) {
		for (int i = 0; i < expectedRange.length / 2; i++) {
			if (expectedRange[2 * i] > checkedRange[2 * i + 1] || checkedRange[2 * i] > expectedRange[2 * i + 1])
				return false;
		}
		return true;
	}

	public ArrayList<LargeCellStore> searchSupportingLargeCells(float[] ExtendArea, partitionTreeNode ptn,
			LargeCellStore currentCell) {
		ArrayList<LargeCellStore> supportingLargeCells = new ArrayList<LargeCellStore>();
		Stack<partitionTreeInternal> stackOfInternals = new Stack<partitionTreeInternal>();
		if (ptn.getClass().getName().endsWith("LargeCellStore")) {
			return supportingLargeCells;
		} else if (ptn.getClass().getName().endsWith("partitionTreeInternal")) {
			stackOfInternals.push((partitionTreeInternal) ptn);
		}
		while (!stackOfInternals.isEmpty()) {
			// check the coordinates of each child node
			ArrayList<partitionTreeNode> tempChildNodes = stackOfInternals.pop().getChildNodes();
			for (int i = 0; i < tempChildNodes.size(); i++) {
				partitionTreeNode tempPTN = tempChildNodes.get(i);
				if (tempPTN.getClass().getName().endsWith("LargeCellStore")
						&& checkRange(ExtendArea, ((LargeCellStore) tempPTN).getCoordinates())
						&& !(tempPTN == currentCell)) {
					// if(tempPTN == currentCell){
					// System.out.println("This is exactly the same cell~ not a
					// support");
					// }
					supportingLargeCells.add((LargeCellStore) tempPTN);
				} else if (tempPTN.getClass().getName().endsWith("partitionTreeInternal")
						&& checkRange(ExtendArea, ((partitionTreeInternal) tempPTN).getCoordinates())) {
					stackOfInternals.push((partitionTreeInternal) tempPTN);
				}
			}
		}
		return supportingLargeCells;
	}

	public LargeCellStore searchLargeCellToStart(float[] boundary, partitionTreeNode ptn, int[] independentDims) {
		float[] newBoundary = new float[independentDims.length * 2];
		for (int i = 0; i < independentDims.length; i++) {
			newBoundary[2 * i] = boundary[2 * independentDims[i]];
			newBoundary[2 * i + 1] = boundary[2 * independentDims[i] + 1];
		}
		Stack<partitionTreeInternal> stackOfInternals = new Stack<partitionTreeInternal>();
		if (ptn.getClass().getName().endsWith("LargeCellStore")) {
			return (LargeCellStore) ptn;
		} else if (ptn.getClass().getName().endsWith("partitionTreeInternal")) {
			stackOfInternals.push((partitionTreeInternal) ptn);
		}
		while (!stackOfInternals.isEmpty()) {
			// check the coordinates of each child node
			ArrayList<partitionTreeNode> tempChildNodes = stackOfInternals.pop().getChildNodes();
			for (int i = 0; i < tempChildNodes.size(); i++) {
				partitionTreeNode tempPTN = tempChildNodes.get(i);
				if (tempPTN.getClass().getName().endsWith("LargeCellStore")
						&& LargeCellStore.checkRange(newBoundary, ((LargeCellStore) tempPTN).getCoordinates())) {
					return (LargeCellStore) tempPTN;
				} else if (tempPTN.getClass().getName().endsWith("partitionTreeInternal")
						&& LargeCellStore.checkRange(newBoundary, ((partitionTreeInternal) tempPTN).getCoordinates())) {
					stackOfInternals.push((partitionTreeInternal) tempPTN);
				}
			}
		}
		return null;
	}

	public float searchKNNInLargeCell(PriorityQueue knnToBound, LargeCellStore currentBucket, float[] boundary,
			HashMap<Long, MetricObject> kNNInfo) {
		for (MetricObject mo : currentBucket.getListOfPoints()) {
			// compute min distance from point to boundary
			float tempMinDist = MinDistFromPointToBoundary(boundary, ((Record) mo.getObj()).getValue());
			if (knnToBound.size() < K) {
				knnToBound.insert(((Record) mo.getObj()).getRId(), tempMinDist);
				kNNInfo.put(((Record) mo.getObj()).getRId(), mo);
			} else if (tempMinDist < knnToBound.getPriority()) {
				long deleteObj = knnToBound.pop();
				kNNInfo.remove(deleteObj);
				knnToBound.insert(((Record) mo.getObj()).getRId(), tempMinDist);
				kNNInfo.put(((Record) mo.getObj()).getRId(), mo);
			}
		}
		return knnToBound.size() == K ? knnToBound.getPriority() : Float.POSITIVE_INFINITY;
		// or separate breakintosmallcells
	}

	public HashMap<Long, MetricObject> computeKNNForBoundary(float[] boundary, partitionTreeNode ptn,
			int[] independentDims, float[] currentPartition) {
		PriorityQueue knnToBound = new PriorityQueue(PriorityQueue.SORT_ORDER_DESCENDING);
		HashMap<Long, MetricObject> knnInfo = new HashMap<Long, MetricObject>();
		// search a large cell that overlaps with this boundary segments
//		System.out.println("Boundary: " + floatArrayToString(boundary));
		LargeCellStore startBucket = searchLargeCellToStart(boundary, ptn, independentDims);
		// System.out.println(startBucket.printCellStoreDetailedInfo());
		// search within the large cell
		float tempKdist = searchKNNInLargeCell(knnToBound, startBucket, boundary, knnInfo);
		// bound an expected area accordingly
		float[] ExtendArea = new float[independentDims.length * 2];
		for (int i = 0; i < independentDims.length; i++) {
			ExtendArea[2 * i] = (float) Math.max(currentPartition[2 * independentDims[i]],
					boundary[2 * independentDims[i]] - tempKdist);
			ExtendArea[2 * i + 1] = (float) Math.min(currentPartition[2 * independentDims[i] + 1],
					boundary[2 * independentDims[i] + 1] + tempKdist);
		}
		// search other supporting large cells
		ArrayList<LargeCellStore> supportingLargeCells = searchSupportingLargeCells(ExtendArea, ptn, startBucket);
		// System.out.println("Supporting Large Cells :" +
		// supportingLargeCells.size());
		for (LargeCellStore supportingCell : supportingLargeCells) {
			if (checkRange(ExtendArea, supportingCell.getCoordinates())) {
				tempKdist = searchKNNInLargeCell(knnToBound, supportingCell, boundary, knnInfo);
				float[] newExtendArea = new float[independentDims.length * 2];
				for (int i = 0; i < independentDims.length; i++) {
					newExtendArea[2 * i] = (float) Math.max(currentPartition[2 * independentDims[i]],
							boundary[2 * independentDims[i]] - tempKdist);
					newExtendArea[2 * i + 1] = (float) Math.min(currentPartition[2 * independentDims[i] + 1],
							boundary[2 * independentDims[i] + 1] + tempKdist);
				}
				ExtendArea = newExtendArea;
			} // end if(checkRange(ExtendArea, supportingCell.getCoordinates()))
		} // end for
		return knnInfo;
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

	public void filterDataPoints(float[] boundary, float maxDist, int[] independentDims, partitionTreeNode ptn,
			float[] currentPartition) {
		LargeCellStore startBucket = searchLargeCellToStart(boundary, ptn, independentDims);
		markPointsAsSupport(startBucket, boundary, maxDist);
		// bound an expected area accordingly
		float[] ExtendArea = new float[independentDims.length * 2];
		for (int i = 0; i < independentDims.length; i++) {
			ExtendArea[2 * i] = (float) Math.max(currentPartition[2 * independentDims[i]],
					boundary[2 * independentDims[i]] - maxDist);
			ExtendArea[2 * i + 1] = (float) Math.min(currentPartition[2 * independentDims[i] + 1],
					boundary[2 * independentDims[i] + 1] + maxDist);
		}
		// search other supporting large cells
		ArrayList<LargeCellStore> supportingLargeCells = searchSupportingLargeCells(ExtendArea, ptn, startBucket);
		// System.out.println("Supporting Large Cells :" +
		// supportingLargeCells.size());
		for (LargeCellStore supportingCell : supportingLargeCells) {
			markPointsAsSupport(supportingCell, boundary, maxDist);
		}
	}

	public void markPointsAsSupport(LargeCellStore currentBucket, float[] boundary, float maxDist) {
		for (MetricObject mo : currentBucket.getListOfPoints()) {
			float[] crds = ((Record) mo.getObj()).getValue();
			float minDistToB = MinDistFromPointToBoundary(boundary, crds);
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

	public float MaxDistFromPointListToBoundary(float[] boundary, HashMap<Long, MetricObject> knnToBoundList) {
		float maxDist = 0.0f;
		for (MetricObject mo : knnToBoundList.values()) {
			maxDist = Math.max(maxDist, MaxDistFromPointToBoundary(boundary, ((Record) mo.getObj()).getValue()));

		}
		return maxDist;
	}

	public void computeSafePruningPoints(ArrayList<MetricObject> listOfPoints,
			HashMap<Integer, float[]> partition_store, int currentPid, partitionTreeNode ptn, int[] independentDims) {
		float[] currentPartition = partition_store.get(currentPid);
		for (Map.Entry<Integer, float[]> adjPartition : partition_store.entrySet()) {
			if (adjPartition.getKey() == currentPid)
				continue;
			// first compute boundary between two partitions
			float[] boundary = boundaryOfTwoPartitions(currentPartition, adjPartition.getValue());
			// System.out.println("Boundary: " + floatArrayToString(boundary));
			// divide the boundary into several boundary segments
			HashSet<String> boundaryList = boundarySegmentGenerator(boundary);
			for (String s : boundaryList) {
				// System.out.println("Segments: " + s);
				// compute knn of the boundary
				HashMap<Long, MetricObject> knnToBoundList = computeKNNForBoundary(StringToFloatArray(s), ptn,
						independentDims, currentPartition);
				// compute maxdistance
				float maxDist = MaxDistFromPointListToBoundary(StringToFloatArray(s), knnToBoundList);
				// System.out.println("MaxDist: " + maxDist);
				// filter points
				filterDataPoints(StringToFloatArray(s), maxDist, independentDims, ptn, currentPartition);
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
