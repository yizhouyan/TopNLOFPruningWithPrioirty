package lof.pruning.secondknn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.mapreduce.Reducer.Context;

import lof.pruning.firstknn.prQuadTree.prQuadInternal;
import lof.pruning.firstknn.prQuadTree.prQuadLeaf;
import metricspace.MetricObject;
import metricspace.MetricObjectMore;
import metricspace.Record;

public abstract class prQuadNodeMore {
	private float[] coordinates;
	private int[] indexInSmallCell;
	private prQuadNodeMore parentNode = null;
	private int[] numSmallCells;
	private float smallCellSize = 0.0f;

	public prQuadNodeMore(float[] xyCoordinates, int[] indexInSmallCell, prQuadNodeMore parentNode, int[] numSmallCells,
			float smallCellSize) {
		this.coordinates = xyCoordinates;
		this.indexInSmallCell = indexInSmallCell;
		this.parentNode = parentNode;
		this.setNumSmallCells(numSmallCells);
		this.smallCellSize = smallCellSize;
	}

	/**
	 * check if these two ranges overlap?
	 * 
	 * @param expectedRange
	 * @param checkedRange
	 * @return
	 */
	boolean checkRange(double[] expectedRange, double[] checkedRange) {
		// check (x1,y2) and (x2,y1)
		if (expectedRange[0] > checkedRange[1] || checkedRange[0] > expectedRange[1])
			return false;
		if (expectedRange[3] < checkedRange[2] || checkedRange[3] < expectedRange[2])
			return false;
		return true;
	}

	public void generateChilden(prQuadInternalMore curPRNode, Stack<prQuadInternalMore> prQuadTree,
			HashMap<prQuadInternalMore, ArrayList<MetricObjectMore>> mapQuadInternalWithPoints,
			ArrayList<prQuadLeafMore> prLeaves, float[] largeCellCoor, int[] numSmallInLarge, int[] independentDims,
			int[] correlatedDims, int K) {
		int count = 0;
		int[][] numCellsPerDim = new int[independentDims.length][2];
		for (int i = 0; i < independentDims.length; i++) {
			numCellsPerDim[i][0] = (int) Math.ceil(curPRNode.getNumSmallCells()[i] / 2);
			numCellsPerDim[i][1] = curPRNode.getNumSmallCells()[i] - numCellsPerDim[i][0];
		}

		// init small cell index (the beginning index of the second one)
		// if we have index(3,9), then the first one contains (3,6) the second
		// one contains(7,9)
		int[] midSmallIndex = new int[independentDims.length];
		for (int i = 0; i < independentDims.length; i++) {
			midSmallIndex[i] = curPRNode.getIndexInSmallCell()[2 * i] + numCellsPerDim[i][0];
		}
		int[][] newIndexInSmall = new int[independentDims.length][4];
		for (int i = 0; i < independentDims.length; i++) {
			newIndexInSmall[i][0] = curPRNode.getIndexInSmallCell()[2 * i];
			newIndexInSmall[i][1] = midSmallIndex[i] - 1;
			newIndexInSmall[i][2] = midSmallIndex[i];
			newIndexInSmall[i][3] = curPRNode.getIndexInSmallCell()[2 * i + 1];
		}

		// new Coordinate Generated
		float[] midCoor = new float[independentDims.length];
		for (int i = 0; i < independentDims.length; i++) {
			midCoor[i] = largeCellCoor[2 * i] + curPRNode.getSmallCellSize() * midSmallIndex[i];
		}
		float[][] newCoor = new float[independentDims.length][3];
		for (int i = 0; i < independentDims.length; i++) {
			newCoor[i][0] = curPRNode.getCoordinates()[2 * i];
			newCoor[i][1] = midCoor[i];
			newCoor[i][2] = curPRNode.getCoordinates()[2 * i + 1];
		}
		// Init each small bucket (dims * 2 in total)
		int numNewBucket = (int) Math.pow(2, independentDims.length);
		ArrayList[] pointsForS = new ArrayList[numNewBucket];
		for (int i = 0; i < numNewBucket; i++)
			pointsForS[i] = new ArrayList<MetricObjectMore>();

		ArrayList<MetricObjectMore> listOfPoints = mapQuadInternalWithPoints.get(curPRNode);
		for (MetricObjectMore mo : listOfPoints) {
			int[] tempIndex = mo.getIndexForSmallCell();
			int result = 0;
			for (int i = 0; i < independentDims.length; i++) {
				int temp = (tempIndex[i] < midSmallIndex[i]) ? 0 : 1;
				result = result + (int) Math.pow(2, i) * temp;
			}
			pointsForS[result].add(mo);
		}
		for (int i = 0; i < numNewBucket; i++) {
			int[] tempIndexEachDim = CalculateIndexPerDim(i, independentDims.length);
			int[] numCellsPerDimPerBucket = new int[independentDims.length];
			for (int j = 0; j < independentDims.length; j++) {
				numCellsPerDimPerBucket[j] = numCellsPerDim[j][tempIndexEachDim[j]];
			}
			for (int j = 0; j < independentDims.length; j++) {
				if (numCellsPerDimPerBucket[j] <= 0)
					continue;
			}
			// check size and num of points inside, if size too small or num
			// of points small, don't divide
			boolean reachMinSizeEachDim = true;
			for (int j = 0; j < independentDims.length; j++) {
				if (numCellsPerDimPerBucket[j] != 1)
					reachMinSizeEachDim = false;
			}
			float[] newConstructed = new float[independentDims.length * 2];
			for (int j = 0; j < independentDims.length; j++) {
				newConstructed[2 * j] = newCoor[j][tempIndexEachDim[j]];
				newConstructed[2 * j + 1] = newCoor[j][tempIndexEachDim[j] + 1];
			}
			int[] newIndexInsideSmall = new int[independentDims.length * 2];
			for (int j = 0; j < independentDims.length; j++) {
				newIndexInsideSmall[2 * j] = newIndexInSmall[j][2 * tempIndexEachDim[j]];
				newIndexInsideSmall[2 * j + 1] = newIndexInSmall[j][2 * tempIndexEachDim[j] + 1];
			}
			if (reachMinSizeEachDim || pointsForS[i].size() < K + 1) {
				prQuadLeafMore newLeaf = new prQuadLeafMore(newConstructed, newIndexInsideSmall, curPRNode,
						numCellsPerDimPerBucket, curPRNode.getSmallCellSize(), pointsForS[i], pointsForS[i].size());
				if (pointsForS[i].size() > 0) {
					curPRNode.addNewChild(newLeaf);
					prLeaves.add(newLeaf);
					// context.getCounter(Counters.SmallCellsNum).increment(1);
				}
			} else {
				// create a internal node and add the arraylist to the
				// hashmap structure
				prQuadInternalMore NewLeaf = new prQuadInternalMore(newConstructed, newIndexInsideSmall, curPRNode,
						numCellsPerDimPerBucket, curPRNode.getSmallCellSize());
				curPRNode.addNewChild(NewLeaf);
				mapQuadInternalWithPoints.put(NewLeaf, pointsForS[i]);

				prQuadTree.push(NewLeaf);
			}
		}
	}

	public static int[] CalculateIndexPerDim(int number, int numIndependentDim) {
		int[] results = new int[numIndependentDim];
		for (int i = numIndependentDim - 1; i >= 0; i--) {
			int temp = (int) Math.pow(2, i);
			results[i] = (int) Math.floor(number / temp);
			number = number - temp * results[i];
		}
		return results;
	}

	public boolean areaInsideSafeArea(float[] safeArea, float[] extendedArea) {
		if (extendedArea[0] >= safeArea[0] && extendedArea[1] <= safeArea[1] && extendedArea[2] >= safeArea[2]
				&& extendedArea[3] <= safeArea[3])
			return true;
		else
			return false;
	}

	public int[] getIndexInSmallCell() {
		return indexInSmallCell;
	}

	public void setIndexInSmallCell(int[] indexInSmallCell) {
		this.indexInSmallCell = indexInSmallCell;
	}

	public float[] getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(float[] xyCoordinates) {
		this.coordinates = xyCoordinates;
	}

	public boolean checkIfParentNull() {
		if (parentNode == null)
			return true;
		else
			return false;
	}

	public prQuadNodeMore getParentNode() {
		return parentNode;
	}

	public void setParentNode(prQuadNodeMore parentNode) {
		this.parentNode = parentNode;
	}

	public float getSmallCellSize() {
		return smallCellSize;
	}

	public void setSmallCellSize(float smallCellSize) {
		this.smallCellSize = smallCellSize;
	}

	public int[] getNumSmallCells() {
		return numSmallCells;
	}

	public void setNumSmallCells(int[] numSmallCells) {
		this.numSmallCells = numSmallCells;
	}
}
