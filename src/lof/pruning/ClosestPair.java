package lof.pruning;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricObject;
import metricspace.MetricSpaceUtility;
import metricspace.Record;
import metricspace.SafeArea;
import util.SQConfig;

public class ClosestPair {
	public static class Pair {
		public IMetric metric = null;
		public double distance = 0.0;
		public boolean canMerge = true;
		public ArrayList<Double> closestPairList = new ArrayList<Double>();

		public partitionTreeNode ptn = null;

		public Pair() {
		}

		public Pair(partitionTreeNode ptn, double distance, boolean canMerge) {
			this.ptn = ptn;
			this.distance = distance;
			this.canMerge = canMerge;
		}

		public Pair(MetricObject point1, MetricObject point2, IMetric metric) {
			this.metric = metric;
			calcDistance(point1, point2);
		}

		public void update(double distance) {
			this.distance = distance;
		}

		public void calcDistance(MetricObject point1, MetricObject point2) {
			this.distance = distance(point1, point2, metric);
		}

		public double computeChiSquareForBucket() {
			double chisquare = 0;
			if (closestPairList.size() <= 1)
				return chisquare;
			else {
				// compute average of closest pair distance
				double average = 0;
				for (double temp : closestPairList)
					average += temp;
				average /= closestPairList.size();

				// compute chi-square
				for (double temp : closestPairList) {
					double f = (temp - average);
					chisquare += f * f;
				}
				chisquare /= average;
				return chisquare;
			}

		}
	}

	public static double distance(MetricObject p1, MetricObject p2, IMetric metric) {
		try {
			return metric.dist(p1.getObj(), p2.getObj());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0.0;
		}
	}

	public static Pair bruteForce(ArrayList<MetricObject> points, float[] coordinates, SafeArea sa, boolean overlapSum,
			IMetric metric) {
		int numPoints = points.size();
		if (numPoints < 2)
			return null;
		Pair pair = new Pair(points.get(0), points.get(1), metric);
		if (numPoints > 2) {
			for (int i = 0; i < numPoints - 1; i++) {
				MetricObject point1 = points.get(i);
				for (int j = i + 1; j < numPoints; j++) {
					MetricObject point2 = points.get(j);
					double distance = distance(point1, point2, metric);
					if (distance < pair.distance)
						pair.update(distance);
				}
			}
		}
		if (overlapSum) {
			float diagnalLength = computeDiagnal(coordinates);
			checkOverlapByDim(sa.getPartitionSize(), coordinates, diagnalLength, sa);
		}
		return pair;
	}

	public static boolean checkOverlapDims(float[] partitionArea, float[] checkedArea) {
		for (int i = 0; i < 4; i++) {
			if (partitionArea[i] == checkedArea[i])
				return true;
		}
		return false;
	}

	public static void checkOverlapByDim(float[] partitionArea, float[] checkedArea, float diagnalLength, SafeArea sa) {
		float[] res = new float[4];
		float[] oldExtendArea = sa.getExtendAbsSize();
		for (int i = 0; i < 4; i++) {
			if (partitionArea[i] == checkedArea[i])
				res[i] = Math.max(diagnalLength, oldExtendArea[i]);
			else
				res[i] = oldExtendArea[i];
		}
		sa.setExtendAbsSize(res);
	}

	public static float computeDiagnal(float[] area) {
		return (float) Math.sqrt(Math.pow((area[1] - area[0]), 2) + Math.pow((area[3] - area[2]), 2));
	}

	public static void sortByX(ArrayList<MetricObject> points) {
		Collections.sort(points, new Comparator<MetricObject>() {
			public int compare(MetricObject point1, MetricObject point2) {
				if (((Record) point1.getObj()).getValue()[0] < ((Record) point2.getObj()).getValue()[0])
					return -1;
				if (((Record) point1.getObj()).getValue()[0] > ((Record) point2.getObj()).getValue()[0])
					return 1;
				return 0;
			}
		});
	}

	public static void sortByY(ArrayList<MetricObject> points) {
		Collections.sort(points, new Comparator<MetricObject>() {
			public int compare(MetricObject point1, MetricObject point2) {
				if (((Record) point1.getObj()).getValue()[1] < ((Record) point2.getObj()).getValue()[1])
					return -1;
				if (((Record) point1.getObj()).getValue()[1] > ((Record) point2.getObj()).getValue()[1])
					return 1;
				return 0;
			}
		});
	}

	public static partitionTreeNode divideAndConquer(ArrayList<MetricObject> points, float[] coordinates,
			ArrayList<LargeCellStore> leaveNodes, SafeArea sa, int K, IMetric metric, IMetricSpace metricspace,
			float threshold) {
		ArrayList<MetricObject> pointsSortedByX = new ArrayList<MetricObject>(points);
		sortByX(pointsSortedByX);
		ArrayList<MetricObject> pointsSortedByY = new ArrayList<MetricObject>(points);
		sortByY(pointsSortedByY);
		Pair tempRes;
		// boolean[] overlapDim = { true, true, true, true };
		boolean overlapSum = true;
		if (coordinates[1] - coordinates[0] >= coordinates[3] - coordinates[2])
			tempRes = divideAndConquerByX(pointsSortedByX, pointsSortedByY, coordinates, leaveNodes, sa, overlapSum, K,
					threshold, metric, metricspace);
		else
			tempRes = divideAndConquerByY(pointsSortedByX, pointsSortedByY, coordinates, leaveNodes, sa, overlapSum, K,
					threshold, metric, metricspace);
		partitionTreeNode ptn;
		if (tempRes.canMerge) {
			// System.out.println("Closest Pair: " + tempRes.distance);
			// create a new Large Cell Store
			ptn = new LargeCellStore(coordinates, points, (float) (tempRes.distance), metric,
					metricspace);
			((LargeCellStore) ptn).computePriorityForLargeCell(true, 0, threshold, K);
			leaveNodes.add((LargeCellStore) ptn);
		} else {
			ptn = tempRes.ptn;
		}
		return ptn;
	}

	private static Pair divideAndConquerByX(ArrayList<MetricObject> pointsSortedByX,
			ArrayList<MetricObject> pointsSortedByY, float[] coordinates, ArrayList<LargeCellStore> leaveNodes,
			SafeArea sa, boolean overlapSum, int K, float threshold, IMetric metric, IMetricSpace metricspace) {
		// System.out.println("Deal With X coordinate");
		int numPoints = pointsSortedByX.size();
		if (numPoints < 10 * K) {
			Pair pairRes = SecondaryDivideAndConquer(pointsSortedByX, pointsSortedByY, coordinates, sa, overlapSum, K,
					metric, metricspace);
			pairRes.closestPairList.add(pairRes.distance);
			return pairRes;
			// return bruteForce(pointsSortedByX, metric);
		}

		// divide the dataset into left and right
		int dividingIndex = numPoints >>> 1;
		ArrayList<MetricObject> leftOfCenter = new ArrayList<MetricObject>(pointsSortedByX.subList(0, dividingIndex));
		ArrayList<MetricObject> rightOfCenter = new ArrayList<MetricObject>(
				pointsSortedByX.subList(dividingIndex, numPoints));
		float centerX = ((Record) rightOfCenter.get(0).getObj()).getValue()[0];
		float[] leftCoordinates = { coordinates[0], centerX, coordinates[2], coordinates[3] };
		float[] rightCoordinates = { centerX, coordinates[1], coordinates[2], coordinates[3] };
		// System.out.println("Coordinates:" + coordinates[0] + "," +
		// coordinates[1]
		// + "," + coordinates[2] + "," + coordinates[3] + "," + centerX);
		// deal with left first (Call divide and Conquer)
		ArrayList<MetricObject> tempList = new ArrayList<MetricObject>(leftOfCenter);
		sortByY(tempList);
		Pair closestPair;

		if (leftCoordinates[1] - leftCoordinates[0] >= leftCoordinates[3] - leftCoordinates[2])
			closestPair = divideAndConquerByX(leftOfCenter, tempList, leftCoordinates, leaveNodes, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), leftCoordinates)), K,
					threshold, metric, metricspace);
		else
			closestPair = divideAndConquerByY(leftOfCenter, tempList, leftCoordinates, leaveNodes, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), leftCoordinates)), K,
					threshold, metric, metricspace);

		// deal with right (Call divide and Conquer)
		tempList.clear();
		tempList.addAll(rightOfCenter);
		sortByY(tempList);
		Pair closestPairRight;
		if (rightCoordinates[1] - rightCoordinates[0] >= rightCoordinates[3] - rightCoordinates[2])
			closestPairRight = divideAndConquerByX(rightOfCenter, tempList, rightCoordinates, leaveNodes, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), rightCoordinates)), K,
					threshold, metric, metricspace);
		else
			closestPairRight = divideAndConquerByY(rightOfCenter, tempList, rightCoordinates, leaveNodes, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), rightCoordinates)), K,
					threshold, metric, metricspace);
		return dealTwoCPs(closestPair, closestPairRight, pointsSortedByY, centerX, coordinates, leftCoordinates,
				rightCoordinates, leftOfCenter, rightOfCenter, leaveNodes, true, metric, metricspace,
				sa.getPartitionSize(), threshold, K);
	}

	private static Pair divideAndConquerByY(ArrayList<MetricObject> pointsSortedByX,
			ArrayList<MetricObject> pointsSortedByY, float[] coordinates, ArrayList<LargeCellStore> leaveNodes,
			SafeArea sa, boolean overlapSum, int K, float threshold, IMetric metric, IMetricSpace metricspace) {
		// System.out.println("Deal With Y coordinate");
		int numPoints = pointsSortedByY.size();
		if (numPoints < 10 * K) {
			Pair pairRes = SecondaryDivideAndConquer(pointsSortedByX, pointsSortedByY, coordinates, sa, overlapSum, K,
					metric, metricspace);
			pairRes.closestPairList.add(pairRes.distance);
			return pairRes;
			// return bruteForce(pointsSortedByY, metric);
		}

		// divide the dataset into left and right
		int dividingIndex = numPoints >>> 1;
		ArrayList<MetricObject> leftOfCenter = new ArrayList<MetricObject>(pointsSortedByY.subList(0, dividingIndex));
		ArrayList<MetricObject> rightOfCenter = new ArrayList<MetricObject>(
				pointsSortedByY.subList(dividingIndex, numPoints));
		float centerY = ((Record) rightOfCenter.get(0).getObj()).getValue()[1];
		float[] leftCoordinates = { coordinates[0], coordinates[1], coordinates[2], centerY };
		float[] rightCoordinates = { coordinates[0], coordinates[1], centerY, coordinates[3] };
		// System.out.println("Coordinates:" + coordinates[0] + "," +
		// coordinates[1]
		// + "," + coordinates[2] + "," + coordinates[3] + "," + centerY);
		// deal with left first (Call divide and Conquer)
		ArrayList<MetricObject> tempList = new ArrayList<MetricObject>(leftOfCenter);
		sortByX(tempList);
		Pair closestPair;
		if (leftCoordinates[1] - leftCoordinates[0] >= leftCoordinates[3] - leftCoordinates[2])
			closestPair = divideAndConquerByX(tempList, leftOfCenter, leftCoordinates, leaveNodes, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), leftCoordinates)), K,
					threshold, metric, metricspace);
		else
			closestPair = divideAndConquerByY(tempList, leftOfCenter, leftCoordinates, leaveNodes, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), leftCoordinates)), K,
					threshold, metric, metricspace);

		// deal with right (Call divide and Conquer)
		tempList.clear();
		tempList.addAll(rightOfCenter);
		sortByX(tempList);
		Pair closestPairRight;
		if (rightCoordinates[1] - rightCoordinates[0] >= rightCoordinates[3] - rightCoordinates[2])
			closestPairRight = divideAndConquerByX(tempList, rightOfCenter, rightCoordinates, leaveNodes, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), rightCoordinates)), K,
					threshold, metric, metricspace);
		else
			closestPairRight = divideAndConquerByY(tempList, rightOfCenter, rightCoordinates, leaveNodes, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), rightCoordinates)), K,
					threshold, metric, metricspace);
		return dealTwoCPs(closestPair, closestPairRight, pointsSortedByX, centerY, coordinates, leftCoordinates,
				rightCoordinates, leftOfCenter, rightOfCenter, leaveNodes, false, metric, metricspace,
				sa.getPartitionSize(), threshold, K);
	}

	private static Pair dealTwoCPs(Pair closestPair, Pair closestPairRight, ArrayList<MetricObject> pointsSortedByXY,
			double centerXY, float[] coordinates, float[] leftCoordinates, float[] rightCoordinates,
			ArrayList<MetricObject> leftOfCenter, ArrayList<MetricObject> rightOfCenter,
			ArrayList<LargeCellStore> leaveNodes, boolean dealX, IMetric metric, IMetricSpace metricspace,
			float[] partitionArea, float threshold, int K) {
		// check if these can be combined
		if (closestPair.canMerge && closestPairRight.canMerge) {
			// if the two closest pair is not that different
			double minCP = Math.min(closestPair.distance, closestPairRight.distance);
			double maxCP = Math.max(closestPair.distance, closestPairRight.distance);
			// System.out.println("Both can merge");
			if ((closestPair.distance <= 1e-10 && closestPairRight.distance <= 1e-10)
					|| (minCP >= 1e-10 && maxCP / minCP <= 10)) {
				closestPair.distance = minCP;
				ArrayList<MetricObject> tempList = new ArrayList<MetricObject>();
				double shortestDistance = closestPair.distance;
				if (dealX) {
					for (MetricObject point : pointsSortedByXY)
						if (Math.abs(centerXY - ((Record) point.getObj()).getValue()[0]) < shortestDistance)
							tempList.add(point);
					for (int i = 0; i < tempList.size() - 1; i++) {
						MetricObject point1 = tempList.get(i);
						for (int j = i + 1; j < tempList.size(); j++) {
							MetricObject point2 = tempList.get(j);
							if ((((Record) point2.getObj()).getValue()[1]
									- ((Record) point1.getObj()).getValue()[1]) >= shortestDistance)
								break;
							double distance = distance(point1, point2, metric);
							if (distance < closestPair.distance) {
								closestPair.update(distance);
								shortestDistance = distance;
							}
						}
					}
					// System.out.println("Update CP: " + closestPair.distance);
				} // end if deal with x-coordinate
				else {
					for (MetricObject point : pointsSortedByXY)
						if (Math.abs(centerXY - ((Record) point.getObj()).getValue()[1]) < shortestDistance)
							tempList.add(point);
					for (int i = 0; i < tempList.size() - 1; i++) {
						MetricObject point1 = tempList.get(i);
						for (int j = i + 1; j < tempList.size(); j++) {
							MetricObject point2 = tempList.get(j);
							if ((((Record) point2.getObj()).getValue()[0]
									- ((Record) point1.getObj()).getValue()[0]) >= shortestDistance)
								break;
							double distance = distance(point1, point2, metric);
							if (distance < closestPair.distance) {
								closestPair.update(distance);
								shortestDistance = distance;
							}
						}
					}
					// System.out.println("Update CP: " + closestPair.distance);
				} // end else
				closestPair.closestPairList.addAll(closestPairRight.closestPairList);
				return closestPair;
			} // end if compare two cp
			else { // both can merge but in fact these two cannot merge because
					// of the cps
					// create two leave nodes
					// System.out.println("CP different, cannot merge");
				LargeCellStore leftLargeCell = new LargeCellStore(leftCoordinates, leftOfCenter,
						(float) (closestPair.distance), metric, metricspace);
				leftLargeCell.computePriorityForLargeCell(
						checkOverlapDims(partitionArea, leftCoordinates),
						closestPair.computeChiSquareForBucket(), threshold, K);
				leaveNodes.add(leftLargeCell);
				LargeCellStore rightLargeCell = new LargeCellStore(rightCoordinates, rightOfCenter,
						(float) (closestPairRight.distance), metric, metricspace);
				rightLargeCell.computePriorityForLargeCell(checkOverlapDims(partitionArea, rightCoordinates),
						closestPairRight.computeChiSquareForBucket(), threshold, K);
				leaveNodes.add(rightLargeCell);
				// then create one internal node and return this internal node
				partitionTreeInternal pti = new partitionTreeInternal(coordinates);
				leftLargeCell.setParentNode(pti);
				rightLargeCell.setParentNode(pti);
				pti.addNewChild(leftLargeCell);
				pti.addNewChild(rightLargeCell);
				return new Pair(pti, 0, false);
			}
		} // end if both can merge
		else if ((!closestPair.canMerge) && closestPairRight.canMerge) {
			// System.out.println("Left can not merge, Right can merge ");
			// change the can merge one to a leave node
			LargeCellStore rightLargeCell = new LargeCellStore(rightCoordinates, rightOfCenter,
					(float) (closestPairRight.distance),  metric, metricspace);
			rightLargeCell.computePriorityForLargeCell(checkOverlapDims(partitionArea, rightCoordinates),
					closestPairRight.computeChiSquareForBucket(), threshold, K);
			leaveNodes.add(rightLargeCell);
			// then create one internal node and return this internal node
			partitionTreeInternal pti = new partitionTreeInternal(coordinates);
			closestPair.ptn.setParentNode(pti);
			rightLargeCell.setParentNode(pti);
			pti.addNewChild(closestPair.ptn);
			pti.addNewChild(rightLargeCell);
			return new Pair(pti, 0, false);
		} else if (closestPair.canMerge && (!closestPairRight.canMerge)) {
			// System.out.println("Left can merge, Right can not merge ");
			LargeCellStore leftLargeCell = new LargeCellStore(leftCoordinates, leftOfCenter,
					(float) (closestPair.distance), metric, metricspace);
			leftLargeCell.computePriorityForLargeCell(checkOverlapDims(partitionArea, leftCoordinates),
					closestPair.computeChiSquareForBucket(), threshold, K);
			leaveNodes.add(leftLargeCell);
			// then create one internal node and return this internal node
			partitionTreeInternal pti = new partitionTreeInternal(coordinates);
			leftLargeCell.setParentNode(pti);
			closestPairRight.ptn.setParentNode(pti);
			pti.addNewChild(leftLargeCell);
			pti.addNewChild(closestPairRight.ptn);
			return new Pair(pti, 0, false);
		} else { // if both cannot merge
			// System.out.println("Both can not merge");
			partitionTreeInternal pti = new partitionTreeInternal(coordinates);
			closestPair.ptn.setParentNode(pti);
			closestPairRight.ptn.setParentNode(pti);
			pti.addNewChild(closestPair.ptn);
			pti.addNewChild(closestPairRight.ptn);
			return new Pair(pti, 0, false);
		}
	}

	public static Pair SecondaryDivideAndConquer(ArrayList<MetricObject> pointsSortedByX,
			ArrayList<MetricObject> pointsSortedByY, float[] coordinates, SafeArea sa, boolean overlapSum, int K,
			IMetric metric, IMetricSpace metricspace) {
		Pair tempRes;
		if (coordinates[1] - coordinates[0] >= coordinates[3] - coordinates[2])
			tempRes = SecondaryDivideAndConquerByX(pointsSortedByX, pointsSortedByY, coordinates, sa, overlapSum, K,
					metric, metricspace);
		else
			tempRes = SecondaryDivideAndConquerByY(pointsSortedByX, pointsSortedByY, coordinates, sa, overlapSum, K,
					metric, metricspace);
		return tempRes;
	}

	private static Pair SecondaryDivideAndConquerByX(ArrayList<MetricObject> pointsSortedByX,
			ArrayList<MetricObject> pointsSortedByY, float[] coordinates, SafeArea sa, boolean overlapSum, int K,
			IMetric metric, IMetricSpace metricspace) {
		// System.out.println("Deal With X coordinate");
		int numPoints = pointsSortedByX.size();
		if (numPoints < 2 * K)
			return bruteForce(pointsSortedByX, coordinates, sa, overlapSum, metric);

		// divide the dataset into left and right
		int dividingIndex = numPoints >>> 1;
		ArrayList<MetricObject> leftOfCenter = new ArrayList<MetricObject>(pointsSortedByX.subList(0, dividingIndex));
		ArrayList<MetricObject> rightOfCenter = new ArrayList<MetricObject>(
				pointsSortedByX.subList(dividingIndex, numPoints));
		float centerX = ((Record) rightOfCenter.get(0).getObj()).getValue()[0];
		float[] leftCoordinates = { coordinates[0], centerX, coordinates[2], coordinates[3] };
		float[] rightCoordinates = { centerX, coordinates[1], coordinates[2], coordinates[3] };

		ArrayList<MetricObject> tempList = new ArrayList<MetricObject>(leftOfCenter);
		sortByY(tempList);
		Pair closestPair;
		if (leftCoordinates[1] - leftCoordinates[0] >= leftCoordinates[3] - leftCoordinates[2])
			closestPair = SecondaryDivideAndConquerByX(leftOfCenter, tempList, leftCoordinates, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), leftCoordinates)), K,
					metric, metricspace);
		else
			closestPair = SecondaryDivideAndConquerByY(leftOfCenter, tempList, leftCoordinates, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), leftCoordinates)), K,
					metric, metricspace);

		// deal with right (Call divide and Conquer)
		tempList.clear();
		tempList.addAll(rightOfCenter);
		sortByY(tempList);
		Pair closestPairRight;
		if (rightCoordinates[1] - rightCoordinates[0] >= rightCoordinates[3] - rightCoordinates[2])
			closestPairRight = SecondaryDivideAndConquerByX(rightOfCenter, tempList, rightCoordinates, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), rightCoordinates)), K,
					metric, metricspace);
		else
			closestPairRight = SecondaryDivideAndConquerByY(rightOfCenter, tempList, rightCoordinates, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), rightCoordinates)), K,
					metric, metricspace);

		double minCP = Math.min(closestPair.distance, closestPairRight.distance);
		closestPair.distance = minCP;
		tempList.clear();
		double shortestDistance = closestPair.distance;

		for (MetricObject point : pointsSortedByY)
			if (Math.abs(centerX - ((Record) point.getObj()).getValue()[0]) < shortestDistance)
				tempList.add(point);
		for (int i = 0; i < tempList.size() - 1; i++) {
			MetricObject point1 = tempList.get(i);
			for (int j = i + 1; j < tempList.size(); j++) {
				MetricObject point2 = tempList.get(j);
				if ((((Record) point2.getObj()).getValue()[1]
						- ((Record) point1.getObj()).getValue()[1]) >= shortestDistance)
					break;
				double distance = distance(point1, point2, metric);
				if (distance < closestPair.distance) {
					closestPair.update(distance);
					shortestDistance = distance;
				}
			}
		}
		return closestPair;
	}

	private static Pair SecondaryDivideAndConquerByY(ArrayList<MetricObject> pointsSortedByX,
			ArrayList<MetricObject> pointsSortedByY, float[] coordinates, SafeArea sa, boolean overlapSum, int K,
			IMetric metric, IMetricSpace metricspace) {
		// System.out.println("Deal With Y coordinate");
		int numPoints = pointsSortedByY.size();
		if (numPoints < 2 * K)
			return bruteForce(pointsSortedByY, coordinates, sa, overlapSum, metric);

		// divide the dataset into left and right
		int dividingIndex = numPoints >>> 1;
		ArrayList<MetricObject> leftOfCenter = new ArrayList<MetricObject>(pointsSortedByY.subList(0, dividingIndex));
		ArrayList<MetricObject> rightOfCenter = new ArrayList<MetricObject>(
				pointsSortedByY.subList(dividingIndex, numPoints));
		float centerY = ((Record) rightOfCenter.get(0).getObj()).getValue()[1];
		float[] leftCoordinates = { coordinates[0], coordinates[1], coordinates[2], centerY };
		float[] rightCoordinates = { coordinates[0], coordinates[1], centerY, coordinates[3] };
		// System.out.println("Coordinates:" + coordinates[0] + "," +
		// coordinates[1]
		// + "," + coordinates[2] + "," + coordinates[3] + "," + centerY);
		// deal with left first (Call divide and Conquer)
		ArrayList<MetricObject> tempList = new ArrayList<MetricObject>(leftOfCenter);
		sortByX(tempList);
		Pair closestPair;
		if (leftCoordinates[1] - leftCoordinates[0] >= leftCoordinates[3] - leftCoordinates[2])
			closestPair = SecondaryDivideAndConquerByX(tempList, leftOfCenter, leftCoordinates, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), leftCoordinates)), K,
					metric, metricspace);
		else
			closestPair = SecondaryDivideAndConquerByY(tempList, leftOfCenter, leftCoordinates, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), leftCoordinates)), K,
					metric, metricspace);

		// deal with right (Call divide and Conquer)
		tempList.clear();
		tempList.addAll(rightOfCenter);
		sortByX(tempList);
		Pair closestPairRight;
		if (rightCoordinates[1] - rightCoordinates[0] >= rightCoordinates[3] - rightCoordinates[2])
			closestPairRight = SecondaryDivideAndConquerByX(tempList, rightOfCenter, rightCoordinates, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), rightCoordinates)), K,
					metric, metricspace);
		else
			closestPairRight = SecondaryDivideAndConquerByY(tempList, rightOfCenter, rightCoordinates, sa,
					(overlapSum == false ? overlapSum : checkOverlapDims(sa.getPartitionSize(), rightCoordinates)), K,
					metric, metricspace);
		double minCP = Math.min(closestPair.distance, closestPairRight.distance);
		closestPair.distance = minCP;
		tempList.clear();
		double shortestDistance = closestPair.distance;

		for (MetricObject point : pointsSortedByX)
			if (Math.abs(centerY - ((Record) point.getObj()).getValue()[1]) < shortestDistance)
				tempList.add(point);
		for (int i = 0; i < tempList.size() - 1; i++) {
			MetricObject point1 = tempList.get(i);
			for (int j = i + 1; j < tempList.size(); j++) {
				MetricObject point2 = tempList.get(j);
				if ((((Record) point2.getObj()).getValue()[0]
						- ((Record) point1.getObj()).getValue()[0]) >= shortestDistance)
					break;
				double distance = distance(point1, point2, metric);
				if (distance < closestPair.distance) {
					closestPair.update(distance);
					shortestDistance = distance;
				}
			}
		}
		return closestPair;
	}

	private static MetricObject parseObject(int key, String strInput, IMetricSpace metricSpace, int num_dims) {
		int partition_id = key;
		int offset = 0;
		Object obj = metricSpace.readObject(strInput.substring(offset, strInput.length()), num_dims);
		return new MetricObject(partition_id, obj);
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		IMetricSpace metricSpace = null;
		IMetric metric = null;
		try {
			metricSpace = MetricSpaceUtility.getMetricSpace("vector");
			metric = MetricSpaceUtility.getMetric("L2Metric");
			metricSpace.setMetric(metric);
		} catch (Exception e) {
			System.out.println("Exception caught");
		}
		// read in objects
		ArrayList<MetricObject> moList = new ArrayList<MetricObject>();
		BufferedReader currentReader = null;
		try {
			currentReader = new BufferedReader(new FileReader("./InputFile"));
			String line;
			while ((line = currentReader.readLine()) != null) {
				/** parse line */
				MetricObject mo = parseObject(1, line, metricSpace, 2);
				moList.add(mo);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println(bruteForce(moList, metric).distance);
		float[] coordinates = { 0, 10000, 0, 10000 };
		ArrayList<LargeCellStore> leaveNodes = new ArrayList<LargeCellStore>();
		SafeArea sa = new SafeArea(coordinates);
//		partitionTreeNode result = divideAndConquer(moList, coordinates, leaveNodes, sa, 3, metric, metricSpace);
		System.out.println("Total number of large buckets: " + leaveNodes.size());
		float[] extendArea = sa.getExtendAbsSize();
		System.out.println("Save Area extend per dim: " + extendArea[0] + "," + extendArea[1] + "," + extendArea[2]
				+ "," + extendArea[3]);
		// HashMap<Long, MetricObject> CanPrunePoints = new HashMap<Long,
		// MetricObject>();
		// int K = 3;

		// // seperate each large bucket into a QuadTree
		// for (int i = 0; i < leaveNodes.size(); i++) {
		// if (leaveNodes.get(i).getNumOfPoints() == 0)
		// continue;
		// else if (leaveNodes.get(i).getNumOfPoints() > K * 5) {
		// leaveNodes.get(i).seperateToSmallCells(CanPrunePoints, 10, K);
		// if (leaveNodes.get(i).isBreakIntoSmallCells())
		// // System.out.println("Build PRQuad Tree successful!");
		// if (!leaveNodes.get(i).isBreakIntoSmallCells() &&
		// leaveNodes.get(i).getNumOfPoints() > K * 20) {
		// // System.out.println("Build another tree........");
		// leaveNodes.get(i).seperateLargeNoPrune(K);
		// }
		// }
		// }

		// float[] partitionExpand = { 0.0f, 0.0f, 0.0f, 0.0f };
		// float[] coor = { 0, 10000, 0, 10000 };
		// float[][] domain = { { 0, 10000 }, { 0, 10000 } };
		// HashMap<Long, MetricObject> TrueKnnPoints = new HashMap<Long,
		// MetricObject>();
		// for (int i = 0; i < leaveNodes.size(); i++) {
		// if (leaveNodes.get(i).isBreakIntoSmallCells()) {
		// // find kNNs within the PR quad tree
		// leaveNodes.get(i).findKnnsWithinPRTree(TrueKnnPoints, leaveNodes, i,
		// result, coor, K, 2, domain);
		// } else if (leaveNodes.get(i).getNumOfPoints() != 0) {
		// // else find kNNs within the large cell
		// leaveNodes.get(i).findKnnsForLargeCell(TrueKnnPoints, leaveNodes, i,
		// result, coor, K, 2, domain);
		// }
		// }

	}

}