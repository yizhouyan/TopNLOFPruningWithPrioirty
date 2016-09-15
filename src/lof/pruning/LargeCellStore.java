package lof.pruning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

import lof.pruning.PriorityQueue;
import lof.pruning.CalKdistanceSecond.Counters;
import lof.pruning.ClosestPair.*;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricObject;
import metricspace.Record;
import metricspace.coreInfoKNNs;
import sampling.CellStore;

public class LargeCellStore extends partitionTreeNode {

	private float x_1;
	private float x_2;
	private float y_1;
	private float y_2;

	/** list of points in the Large grid cell */
	private ArrayList<MetricObject> listOfPoints;

	/** Closest pair distance inside the large cell */
	private float cpDist;

	/** Number of points inside the Large cell */
	private int numOfPoints;

	/** Break up into small cells */
	private float smallCellSize;

	private int numSmallCellsX = 0;

	private int numSmallCellsY = 0;

	private IMetric metric = null;

	private IMetricSpace metricSpace = null;

	private boolean breakIntoSmallCells = false;

	private boolean InsidesafeArea = false;

	private prQuadInternal rootForPRTree;
	// save leaves that cannot be pruned
	ArrayList<prQuadLeaf> prLeaves;
	private int indexForLeaveNodesList = -1;

	public LargeCellStore(float x_1, float x_2, float y_1, float y_2, IMetric metric, IMetricSpace metricspace) {
		this.x_1 = x_1;
		this.x_2 = x_2;
		this.y_1 = y_1;
		this.y_2 = y_2;
		listOfPoints = new ArrayList<MetricObject>();
		cpDist = Float.MAX_VALUE;
		numOfPoints = 0;
		this.metric = metric;
		this.metricSpace = metricspace;
	}

	public LargeCellStore(float[] coordinates, ArrayList<MetricObject> listOfPoints, float cpDist,
			int indexForLeaveNodesList, IMetric metric, IMetricSpace metricspace) {
		this.x_1 = coordinates[0];
		this.x_2 = coordinates[1];
		this.y_1 = coordinates[2];
		this.y_2 = coordinates[3];
		this.listOfPoints = listOfPoints;
		this.numOfPoints = this.listOfPoints.size();
		this.cpDist = cpDist;
		this.setIndexForLeaveNodesList(indexForLeaveNodesList);
		this.metric = metric;
		this.metricSpace = metricspace;
	}

	public int getNumSmallCellsX() {
		return numSmallCellsX;
	}

	public void setNumSmallCellsX(int numSmallCellsX) {
		this.numSmallCellsX = numSmallCellsX;
	}

	public int getNumSmallCellsY() {
		return numSmallCellsY;
	}

	public void setNumSmallCellsY(int numSmallCellsY) {
		this.numSmallCellsY = numSmallCellsY;
	}

	public void addPoints(MetricObject newpoint) {
		listOfPoints.add(newpoint);
		numOfPoints++;
	}

	public ArrayList<MetricObject> getListOfPoints() {
		return listOfPoints;
	}

	public void seperateToSmallCells(HashMap<Long, MetricObject> CanPrunePoints, int indexOfLeaveNodes, float threshold,
			int K, float[] safeArea, partitionTreeNode ptn, float [] partition_store) {
		double LargeCellRangeX = x_2 - x_1;
		double LargeCellRangeY = y_2 - y_1;

		// smallCellSize using closest pair and threshold
		smallCellSize = (float) (threshold * cpDist / (2 * Math.sqrt(2)));
		// cell calculated as average cell size
		float smallCellSize_predict = (float) Math.sqrt((LargeCellRangeX * LargeCellRangeY * K / numOfPoints));
		// if the small cell size too small, then don't use this size to build
		// PRQuadTree
		if (smallCellSize < smallCellSize_predict / 30) {
			// context.getCounter(lof.pruning.LargeCellBasedKnnFind.Counters.SmallCP).increment(1);
			return;
		}
		// if the small cell size too large, use predicted cell size to build
		// PRQuadTree
		if (smallCellSize > smallCellSize_predict / 5) {
			smallCellSize = smallCellSize_predict / 5;
		}

		// float avgDist = (float) Math.sqrt((LargeCellRangeX * LargeCellRangeY)
		// / numOfPoints);
		// if (cpDist < avgDist / 30)
		// System.out.println("Information(False): " + cpDist + "," + avgDist +
		// "," + numOfPoints);
		// else
		// System.out.println("Information(True): " + cpDist + "," + avgDist +
		// "," + numOfPoints);
		// if (numOfPoints <= K) {
		// return;
		// }

		if (smallCellSize >= LargeCellRangeX || smallCellSize >= LargeCellRangeY) {
			/*
			 * if(numOfPoints > K){
			 * context.getCounter(Counters.CanPrune).increment(numOfPoints); }
			 */
			return;
		}

		// calculate how many small cells for each partition per dimension
		numSmallCellsX = (int) Math.ceil(LargeCellRangeX / smallCellSize);
		numSmallCellsY = (int) Math.ceil(LargeCellRangeY / smallCellSize);

		// if (cpDist < avgDist / 30)
		// return;

		boolean boundaryCanPrune;
		// if only one large cell store exists in the partition
		if (ptn.getClass().getName().endsWith("LargeCellStore"))
			boundaryCanPrune = false;
		else {
			// Query surrounding buckets and compare closest pair
			float[] expectedSupportingArea = { (float) (x_1 - 3 * Math.sqrt(2) * smallCellSize),
					(float) (x_2 + 3 * Math.sqrt(2) * smallCellSize), (float) (y_1 - 3 * Math.sqrt(2) * smallCellSize),
					(float) (y_2 + 3 * Math.sqrt(2) * smallCellSize) };
			if (insideCheckedArea(partition_store, expectedSupportingArea)) {
				boundaryCanPrune = QuerySurroundingBucketsForCP(ptn, expectedSupportingArea);
			} else {
				boundaryCanPrune = false;
			}
		}
		if (boundaryCanPrune) {
			if (numSmallCellsX < 2 || numSmallCellsY < 2) {
				return;
			}
		} else {
			if (numSmallCellsX < 10 || numSmallCellsY < 10) {
				return;
			}
		}
		
		breakIntoSmallCells = true;

		for (MetricObject mo : listOfPoints) {
			Record record = (Record) mo.getObj();
			float tempx = record.getValue()[0];
			float tempy = record.getValue()[1];
			int tempIndexX = (int) (Math.floor(tempx - x_1) / (smallCellSize + Float.MIN_VALUE));
			int tempIndexY = (int) (Math.floor(tempy - y_1) / (smallCellSize + Float.MIN_VALUE));
			int[] indexInSmall = { tempIndexX, tempIndexY };
			mo.setIndexForSmallCell(indexInSmall);
		}
		// build up PR quadtree
		float[] largeCellCoor = { x_1, x_2, y_1, y_2 };
		buildPRQuadTree(CanPrunePoints, numSmallCellsX, numSmallCellsY, smallCellSize, listOfPoints, numOfPoints,
				largeCellCoor, indexOfLeaveNodes, K, true, safeArea,boundaryCanPrune);
	}

	public boolean QuerySurroundingBucketsForCP(partitionTreeNode ptn, float[] expectedRange) {
		Stack<partitionTreeInternal> partitionTree = new Stack<partitionTreeInternal>();
		partitionTree.push((partitionTreeInternal) ptn);
		while (!partitionTree.isEmpty()) {
			partitionTreeInternal tempInternal = partitionTree.pop();
			ArrayList<partitionTreeNode> tempChildNodes = tempInternal.getChildNodes();
			// check children
			for (int i = 0; i < tempChildNodes.size(); i++) {
				if (tempChildNodes.get(i).getClass().getName().endsWith("partitionTreeInternal")) {
					float[] tempCoordinates = ((partitionTreeInternal) tempChildNodes.get(i)).getCoordinates();
					if (checkRange(expectedRange, tempCoordinates)) {
						partitionTree.push((partitionTreeInternal) tempChildNodes.get(i));
					}
				} else if (tempChildNodes.get(i).getClass().getName().endsWith("LargeCellStore")) {
					float[] tempCoordinates = ((LargeCellStore) tempChildNodes.get(i)).getCoordinates();
					if (checkRange(expectedRange, tempCoordinates)
							&& ((LargeCellStore) tempChildNodes.get(i)).getCpDist() < this.cpDist) {
						return false;
					}
				} else {
					System.out.println("Unknown Bucket Node Type!");
				}
			}
		}
		return true;
	}

	public void seperateLargeNoPrune(int K, int indexOfLeaveNodes, float[] safeArea) {
		double LargeCellRangeX = x_2 - x_1;
		double LargeCellRangeY = y_2 - y_1;
		smallCellSize = (float) Math.sqrt((LargeCellRangeX * LargeCellRangeY * 4 * K / numOfPoints));

		if (smallCellSize >= LargeCellRangeX || smallCellSize >= LargeCellRangeY) {
			return;
		}
		// calculate how many small cells for each partition per dimension
		numSmallCellsX = (int) Math.ceil(LargeCellRangeX / smallCellSize);
		numSmallCellsY = (int) Math.ceil(LargeCellRangeY / smallCellSize);

		breakIntoSmallCells = true;

		for (MetricObject mo : listOfPoints) {
			Record record = (Record) mo.getObj();
			float tempx = record.getValue()[0];
			float tempy = record.getValue()[1];
			int tempIndexX = (int) (Math.floor(tempx - x_1) / (smallCellSize + Float.MIN_VALUE));
			int tempIndexY = (int) (Math.floor(tempy - y_1) / (smallCellSize + Float.MIN_VALUE));
			int[] indexInSmall = { tempIndexX, tempIndexY };
			mo.setIndexForSmallCell(indexInSmall);
		}

		// build up PR quadtree
		float[] largeCellCoor = { x_1, x_2, y_1, y_2 };
		buildPRQuadTree(null, numSmallCellsX, numSmallCellsY, smallCellSize, listOfPoints, numOfPoints, largeCellCoor,
				indexOfLeaveNodes, K, false, safeArea,false);
	}

	/**
	 * build up PR quad tree with information from the large cell
	 * 
	 * @param numSmallCellsX
	 * @param numSmallCellsY
	 * @param smallCellSize
	 * @param listOfPoints
	 * @param numOfPoints
	 * @param largeCellCoor
	 * @return root of PR QuadTree
	 */
	public void buildPRQuadTree(HashMap<Long, MetricObject> CanPrunePoints, int numSmallCellsX, int numSmallCellsY,
			float smallCellSize, ArrayList<MetricObject> listOfPoints, int numOfPoints, float[] largeCellCoor,
			int indexOfLeaveNodes, int K, boolean withPrune, float[] safeArea, boolean boundaryCanPrune) {
		// init root
		int[] indexRangeInSmallCell = { 0, numSmallCellsX - 1, 0, numSmallCellsY - 1 };
		rootForPRTree = new prQuadInternal(largeCellCoor, indexRangeInSmallCell, null, numSmallCellsX, numSmallCellsY,
				smallCellSize);
		Stack<prQuadInternal> prQuadTree = new Stack<prQuadInternal>();
		HashMap<prQuadInternal, ArrayList<MetricObject>> mapQuadInternalWithPoints = new HashMap<prQuadInternal, ArrayList<MetricObject>>();
		mapQuadInternalWithPoints.put(rootForPRTree, listOfPoints);
		// save leaves in the pr tree
		prLeaves = new ArrayList<prQuadLeaf>();
		prQuadTree.push(rootForPRTree);
		int count = 0;
		while (!prQuadTree.empty()) {
			/**
			 * pop up the quad node and divide into 4 parts check if each part
			 * contains enough points if contains K+1 points, create a
			 * prQuadInternal and push to stack if contains less than K points,
			 * create a prQuadLeaf and save the pointer if can not divide (reach
			 * minimum size), create prQuadLeaf and save the pointer
			 */
			prQuadInternal curPRNode = prQuadTree.pop();
			curPRNode.generateChilden(CanPrunePoints, curPRNode, prQuadTree, mapQuadInternalWithPoints, prLeaves,
					largeCellCoor, numSmallCellsX, numSmallCellsY, indexOfLeaveNodes, K, withPrune, InsidesafeArea,
					safeArea, boundaryCanPrune);
			// System.out.println("Father: "+ curPRNode.printPRQuadNode());
			// for(prQuadNode tempNode: curPRNode.getChildNodes()){
			// System.out.println("Child: "+ tempNode.printPRQuadNode());
			// }
		}
	}

	public PriorityQueue traverseLargeCell(PriorityQueue pq, MetricObject curPoint, LargeCellStore large_cell_store,
			int K) {
		// traverse points
		float dist = 0.0f;
		float theta;
		if (pq.size() > 0)
			theta = pq.getPriority();
		else
			theta = Float.POSITIVE_INFINITY;
		for (int i = 0; i < large_cell_store.getNumOfPoints(); i++) {
			MetricObject o_S = large_cell_store.getListOfPoints().get(i);
			if (((Record) o_S.getObj()).getRId() == ((Record) curPoint.getObj()).getRId()) {
				continue;
			} else if (o_S.getType() == 'C')
				continue;
			else {
				try {
					dist = metric.dist(curPoint.getObj(), o_S.getObj());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (pq.size() < K) {
					pq.insert(metricSpace.getID(o_S.getObj()), dist);
					theta = pq.getPriority();
				} else if (dist < theta) {
					pq.pop();
					pq.insert(metricSpace.getID(o_S.getObj()), dist);
					theta = pq.getPriority();
				}
			}
		} // end for
			// System.out.println("Point id: " + ((Record)
			// curPoint.getObj()).getRId() + " Point Coordinates: "
			// + ((Record) curPoint.getObj()).getValue()[0] + "," + ((Record)
			// curPoint.getObj()).getValue()[1]
			// + " theta: " + theta);
		return pq;
	}

	public PriorityQueue findKnns(PriorityQueue pq, MetricObject curPoint, prQuadLeaf curCheckLeaf, int K) {
		// traverse points
		float dist = 0.0f;
		float theta;
		if (pq.size() > 0)
			theta = pq.getPriority();
		else
			theta = Float.POSITIVE_INFINITY;
		for (int i = 0; i < curCheckLeaf.getNumPoints(); i++) {
			MetricObject o_S = curCheckLeaf.getListOfPoints().get(i);
			if (((Record) o_S.getObj()).getRId() == ((Record) curPoint.getObj()).getRId()) {
				continue;
			} else if (o_S.getType() == 'C')
				continue;
			else {
				try {
					dist = metric.dist(curPoint.getObj(), o_S.getObj());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (pq.size() < K) {
					pq.insert(metricSpace.getID(o_S.getObj()), dist);
					theta = pq.getPriority();
				} else if (dist < theta) {
					pq.pop();
					pq.insert(metricSpace.getID(o_S.getObj()), dist);
					theta = pq.getPriority();
				}
			}
		} // end for
			// System.out.println("Point id: " + ((Record)
			// curPoint.getObj()).getRId() + " Point Coordinates: "
			// + ((Record) curPoint.getObj()).getValue()[0] + "," + ((Record)
			// curPoint.getObj()).getValue()[1]
			// + " theta: " + theta);
		return pq;
	}

	boolean checkRange(float[] expectedRange, float[] checkedRange) {
		// check (x1,y2) and (x2,y1)
		if (expectedRange[0] > checkedRange[1] || checkedRange[0] > expectedRange[1])
			return false;
		if (expectedRange[3] < checkedRange[2] || checkedRange[3] < expectedRange[2])
			return false;
		return true;
	}

	/**
	 * check if the extended area is in checked area
	 * 
	 * @param checkedArea
	 * @param extendedArea
	 * @return
	 */
	boolean insideCheckedArea(float[] checkedArea, float[] extendedArea) {
		if (extendedArea[0] >= checkedArea[0] && extendedArea[1] <= checkedArea[1] && extendedArea[2] >= checkedArea[2]
				&& extendedArea[3] <= checkedArea[3])
			return true;
		else
			return false;
	}

	public void findKnnsWithinOneCell(PriorityQueue pq, MetricObject curPoint, prQuadLeaf curLeaf,
			LargeCellStore large_cell_store, int K) {
		float[] curPointCoor = ((Record) curPoint.getObj()).getValue();
		float kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;
		Stack<prQuadNode> checkWithinOneTree = new Stack<prQuadNode>();
		prQuadNode tempCheckBreakNode = curLeaf;
		float[] extendArea = { Math.max(large_cell_store.x_1, curPointCoor[0] - kdist),
				Math.min(large_cell_store.x_2, curPointCoor[0] + kdist),
				Math.max(large_cell_store.y_1, curPointCoor[1] - kdist),
				Math.min(large_cell_store.y_2, curPointCoor[1] + kdist) };
		float[] checkedCoordinates = tempCheckBreakNode.getXyCoordinates();
		if (insideCheckedArea(checkedCoordinates, extendArea)) {
			return;
		}
		while (tempCheckBreakNode.getParentNode() != null) {
			// first add the parent node
			checkWithinOneTree.push(tempCheckBreakNode.getParentNode());
			// System.out.println("TempCheckBreakNode: "+
			// tempCheckBreakNode.printPRQuadNode());
			// then add brothers
			for (prQuadNode brother : ((prQuadInternal) tempCheckBreakNode.getParentNode()).getChildNodes()) {
				// System.out.println("Brother: "+ brother.printPRQuadNode());
				if (brother.equals(tempCheckBreakNode)) /////////
					continue;
				else if (checkRange(extendArea, brother.getXyCoordinates())) {
					checkWithinOneTree.push(brother);
				} else
					continue;
			}
			// traverse the stack until has only one element(the parent) left,
			// all brother traversed
			while (checkWithinOneTree.size() > 1) {
				prQuadNode tempNode = checkWithinOneTree.pop();
				if (tempNode.getClass().getName().endsWith("prQuadLeaf")) {
					if (tempNode.equals(curLeaf)) {
						continue;
					}
					pq = findKnns(pq, curPoint, (prQuadLeaf) tempNode, K);
					kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;
					// update extend area
					float[] newExtendArea = { Math.max(large_cell_store.x_1, curPointCoor[0] - kdist),
							Math.min(large_cell_store.x_2, curPointCoor[0] + kdist),
							Math.max(large_cell_store.y_1, curPointCoor[1] - kdist),
							Math.min(large_cell_store.y_2, curPointCoor[1] + kdist) };
					extendArea = newExtendArea;
					if (insideCheckedArea(checkedCoordinates, extendArea)) {
						return;
					}
				} else {
					// add children
					for (prQuadNode children : ((prQuadInternal) tempNode).getChildNodes()) {
						if (children.equals(curLeaf))
							continue;
						else if (checkRange(extendArea, children.getXyCoordinates())) {
							checkWithinOneTree.push(children);
						} else
							continue;
					}
				} // end else
			} // end while
				// stack size == 1 only one parent left, all brothers traversed
			tempCheckBreakNode = checkWithinOneTree.pop(); // let parent be the
															// new node
			checkedCoordinates = tempCheckBreakNode.getXyCoordinates();
			if (insideCheckedArea(checkedCoordinates, extendArea)) {
				return;
			}
		} // end while

	}

	public void savePriorityQueueToKNN(PriorityQueue pq, MetricObject curPoint, boolean trueKnn, float expandDist,
			int K) {
		if (pq.size() != K) {
			System.out.println("Less than K points in Priority Queue");
			curPoint.setCanPrune(true);
			return;
		}
		curPoint.setKdist(pq.getPriority());
		curPoint.getKnnInDetail().clear();
		float NNDist = Float.MAX_VALUE;
		while (pq.size() > 0) {
			curPoint.getKnnInDetail().put(pq.getValue(), pq.getPriority());
			// System.out.println("KNN information: "+ pq.getValue() + "," +
			// pq.getPriority());
			NNDist = Math.min(NNDist, pq.getPriority());
			pq.pop();
		}
		curPoint.setNearestNeighborDist(NNDist);
		// curPoint.setExpandDist(expandDist);
		if (trueKnn)
			curPoint.setType('T');
		else
			curPoint.setType('F');
	}

	public float[] boundPartitionSupport(HashMap<Long, MetricObject> TrueKnnPoints, MetricObject o_R, int num_dims,
			float[] partition_store, float[][] domains) {
		Record currentPoint = (Record) o_R.getObj();
		float[] currentPointCoor = currentPoint.getValue();
		float currentKDist = o_R.getKdist();
		boolean tag = true;
		float[] expandDist = { 0.0f, 0.0f, 0.0f, 0.0f };
		for (int i = 0; i < num_dims; i++) {
			float minCurrent = Math.max(domains[i][0], currentPointCoor[i] - currentKDist);
			float maxCurrent = Math.min(domains[i][1] - Float.MIN_VALUE, currentPointCoor[i] + currentKDist);
			if (minCurrent < partition_store[2 * i]) {
				tag = false;
				expandDist[2 * i] = Math.max(expandDist[2 * i], Math.abs(partition_store[2 * i] - minCurrent));
			}
			if (maxCurrent > partition_store[2 * i + 1]) {
				tag = false;
				expandDist[2 * i + 1] = Math.max(expandDist[2 * i + 1],
						Math.abs(partition_store[2 * i + 1] - maxCurrent));
			} else if (maxCurrent == partition_store[2 * i + 1]) {
				tag = false;
				expandDist[2 * i + 1] = Math.max(Float.MIN_VALUE, expandDist[2 * i + 1]);
			}
		}
		if (tag == false) {
			o_R.setType('F');
		} else {
			o_R.setType('T');
			TrueKnnPoints.put(((Record) o_R.getObj()).getRId(), o_R);
		}
		// o_R.setExpandDist(expandDist);
		return expandDist;
	}

	/**
	 * search the partitionTreeNode and find supporting large cells
	 * 
	 * @param ExtendArea
	 *            the coordinate of the point's extended area
	 * @param ptn
	 *            the root node of the partition tree (binary tree)
	 * @param currentCell
	 *            current LargeCellStore, supportingLargeCells will not include
	 *            current cell
	 * @return the supporting cells for the current point
	 */
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

	public float[] findKnnsForOnePoint(HashMap<Long, MetricObject> TrueKnnPoints, MetricObject curPoint,
			prQuadLeaf curLeaf, ArrayList<LargeCellStore> large_cell_store, LargeCellStore currentLeafNode,
			partitionTreeNode ptn, float[] partition_store, int K, int num_dims, float[][] domains) {
		float kdist = Float.POSITIVE_INFINITY;
		PriorityQueue pq = new PriorityQueue(PriorityQueue.SORT_ORDER_DESCENDING);

		// first load in original knns if any
		if (curPoint.getKnnInDetail().size() != 0) {
			for (Map.Entry<Long, Float> tempentry : curPoint.getKnnInDetail().entrySet()) {
				long curkey = tempentry.getKey();
				float curvalue = tempentry.getValue();
				pq.insert(curkey, curvalue);
			}
		}
		kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;

		// first find kNNs within the large cell and bound a partition area for
		// largeCell
		// first find kNNs within the leaf
		pq = findKnns(pq, curPoint, curLeaf, K);
		kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;

		// then find KNNs within the large cell
		findKnnsWithinOneCell(pq, curPoint, curLeaf, currentLeafNode, K);
		kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;
		// System.out.println("old kdistance: " + kdist);
		// check if kNNs exceeds the large cell
		float largeCellExpand = calExtendDistance(currentLeafNode, curPoint, kdist);
		// if not exceed the large cell, don't need to traverse other large
		// cells
		if (largeCellExpand == 0) {
			savePriorityQueueToKNN(pq, curPoint, true, 0, K);
			TrueKnnPoints.put(((Record) curPoint.getObj()).getRId(), curPoint);
			float[] partitionExpand = { 0.0f, 0.0f, 0.0f, 0.0f };
			return partitionExpand;
		}

		// if exceed the large cell, traverse nearby large cells
		// include more supporting cells
		float[] curPointCoor = ((Record) curPoint.getObj()).getValue();
		float[] ExtendArea = { Math.max(partition_store[0], curPointCoor[0] - kdist),
				Math.min(partition_store[1], curPointCoor[0] + kdist),
				Math.max(partition_store[2], curPointCoor[1] - kdist),
				Math.min(partition_store[3], curPointCoor[1] + kdist) };
		ArrayList<LargeCellStore> supportingLargeCells = searchSupportingLargeCells(ExtendArea, ptn, currentLeafNode);
		// System.out.println("Size of Support:" + supportingLargeCells.size());
		// for each supporting cell, traverse until not exceed the checked area
		for (LargeCellStore supportingCell : supportingLargeCells) {
			if (checkRange(ExtendArea, supportingCell.getCoordinates())) {
				// find a leaf to start
				if (supportingCell.breakIntoSmallCells) {
					prQuadLeaf tempLeaf = RangeQuery(supportingCell.getRootForPRTree(), ExtendArea);
					if (tempLeaf != null) {
						// then find KNNs within the large cell
						findKnnsWithinOneCell(pq, curPoint, tempLeaf, supportingCell, K);
						kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;
						float[] newExtendArea = { Math.max(partition_store[0], curPointCoor[0] - kdist),
								Math.min(partition_store[1], curPointCoor[0] + kdist),
								Math.max(partition_store[2], curPointCoor[1] - kdist),
								Math.min(partition_store[3], curPointCoor[1] + kdist) };
						ExtendArea = newExtendArea;
					}
				} // end if
				else { // traverse Large cell
					traverseLargeCell(pq, curPoint, supportingCell, K);
					kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;
					float[] newExtendArea = { Math.max(partition_store[0], curPointCoor[0] - kdist),
							Math.min(partition_store[1], curPointCoor[0] + kdist),
							Math.max(partition_store[2], curPointCoor[1] - kdist),
							Math.min(partition_store[3], curPointCoor[1] + kdist) };
					ExtendArea = newExtendArea;
				}
			} // end if(checkRange(ExtendArea, supportingCell.getCoordinates()))
		} // end for
			// System.out.println("new kdistance: " + kdist);
			// bound supporting area for the partition
		savePriorityQueueToKNN(pq, curPoint, false, 0, K);
		float[] partitionExpand = boundPartitionSupport(TrueKnnPoints, curPoint, num_dims, partition_store, domains);
		return partitionExpand;
	}

	public prQuadLeaf findLeafWithSmallCellIndex(prQuadInternal prRoot, int xx, int yy) {
		Stack<prQuadInternal> prQuadTreeInternal = new Stack<prQuadInternal>();
		prQuadTreeInternal.push(prRoot);
		// System.out.println(xx + "," + yy);
		while (!prQuadTreeInternal.empty()) {
			prQuadInternal curPRNode = prQuadTreeInternal.pop();
			// traverse 4 childs and save to the stack if inside the expecting
			// range
			for (prQuadNode tempNode : curPRNode.getChildNodes()) {
				// check range
				// System.out.println(tempNode.getIndexInSmallCell()[0] + "," +
				// tempNode.getIndexInSmallCell()[1] + ","
				// + tempNode.getIndexInSmallCell()[2] + "," +
				// tempNode.getIndexInSmallCell()[3] + ",");
				if (tempNode.getIndexInSmallCell()[0] <= xx && xx <= tempNode.getIndexInSmallCell()[1]
						&& tempNode.getIndexInSmallCell()[2] <= yy && yy <= tempNode.getIndexInSmallCell()[3]) {
					if (tempNode.getClass().getName().endsWith("prQuadInternal")) {
						prQuadTreeInternal.push((prQuadInternal) tempNode);
					} else { // leaf
						return (prQuadLeaf) tempNode;
					}
				} else
					continue;
			}
		}
		return null;
	}

	public prQuadLeaf RangeQuery(prQuadInternal prRoot, float[] expectedRange) {
		Stack<prQuadInternal> prQuadTreeInternal = new Stack<prQuadInternal>();
		prQuadTreeInternal.push(prRoot);
		while (!prQuadTreeInternal.empty()) {
			prQuadInternal curPRNode = prQuadTreeInternal.pop();
			// traverse 4 childs and save to the stack if inside the expecting
			// range
			for (prQuadNode tempNode : curPRNode.getChildNodes()) {
				// check range
				if (!checkRange(expectedRange, tempNode.getXyCoordinates()))
					continue;
				if (tempNode.getClass().getName().endsWith("prQuadInternal")) {
					prQuadTreeInternal.push((prQuadInternal) tempNode);
				} else { // leaf
					return (prQuadLeaf) tempNode;
				}
			}
		}
		return null;
	}

	public float calExtendDistance(LargeCellStore large_cell_store, MetricObject curPoint, float kdist) {
		float extendDist = 0.0f;
		float[] curPointCoor = ((Record) curPoint.getObj()).getValue();
		extendDist = (float) Math.max(extendDist, large_cell_store.x_1 - (curPointCoor[0] - kdist));
		extendDist = (float) Math.max(extendDist, (curPointCoor[0] + kdist) - large_cell_store.x_2);
		extendDist = (float) Math.max(extendDist, large_cell_store.y_1 - (curPointCoor[1] - kdist));
		extendDist = (float) Math.max(extendDist, (curPointCoor[1] + kdist) - large_cell_store.y_2);
		return extendDist;
	}

	public float[] findKnnsWithinPRTree(HashMap<Long, MetricObject> TrueKnnPoints,
			ArrayList<LargeCellStore> large_cell_store, int xx, partitionTreeNode ptn, float[] partition_store, int K,
			int num_dims, float[][] domains) {
		float[] partitionExpand = { 0.0f, 0.0f, 0.0f, 0.0f };
		// find kNNs for each point in the prLeaves
		for (prQuadLeaf curLeaf : this.prLeaves) {
			for (MetricObject curPoint : curLeaf.getListOfPoints()) {
				partitionExpand = LargeCellBasedKnnFind.CellBasedKNNFinderReducer.maxOfTwoFloatArray(partitionExpand,
						findKnnsForOnePoint(TrueKnnPoints, curPoint, curLeaf, large_cell_store,
								large_cell_store.get(xx), ptn, partition_store, K, num_dims, domains));
			}
		}
		return partitionExpand;
	}

	public float[] findKnnsForLargeCell(HashMap<Long, MetricObject> TrueKnnPoints,
			ArrayList<LargeCellStore> large_cell_store, int xx, partitionTreeNode ptn, float[] partition_store, int K,
			int num_dims, float[][] domains) {
		float[] partitionExpand = { 0.0f, 0.0f, 0.0f, 0.0f };
		// find knns for each point in the large cell
		for (MetricObject curPoint : large_cell_store.get(xx).getListOfPoints()) {
			partitionExpand = LargeCellBasedKnnFind.CellBasedKNNFinderReducer.maxOfTwoFloatArray(partitionExpand,
					findKnnsForOnePointInLargeCell(TrueKnnPoints, curPoint, large_cell_store, xx, ptn, partition_store,
							K, num_dims, domains));
		}
		return partitionExpand;
	}

	public float[] findKnnsForOnePointInLargeCell(HashMap<Long, MetricObject> TrueKnnPoints, MetricObject curPoint,
			ArrayList<LargeCellStore> large_cell_store, int xx, partitionTreeNode ptn, float[] partition_store, int K,
			int num_dims, float[][] domains) {

		float kdist = Float.POSITIVE_INFINITY;
		PriorityQueue pq = new PriorityQueue(PriorityQueue.SORT_ORDER_DESCENDING);
		// first load in original knns if any
		if (curPoint.getKnnInDetail().size() != 0) {
			for (Map.Entry<Long, Float> tempentry : curPoint.getKnnInDetail().entrySet()) {
				long curkey = tempentry.getKey();
				float curvalue = tempentry.getValue();
				pq.insert(curkey, curvalue);
			}
		}
		kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;

		// first traverse the large cell
		traverseLargeCell(pq, curPoint, large_cell_store.get(xx), K);
		kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;
		// check if kNNs exceeds the large cell
		float largeCellExpand = calExtendDistance(large_cell_store.get(xx), curPoint, kdist);
		// if not exceed the large cell, don't need to traverse other large
		// cells
		if (largeCellExpand == 0) {
			savePriorityQueueToKNN(pq, curPoint, true, 0, K);
			TrueKnnPoints.put(((Record) curPoint.getObj()).getRId(), curPoint);
			float[] partitionExpand = { 0.0f, 0.0f, 0.0f, 0.0f };
			return partitionExpand;
		}
		// if exceed the bucket, traverse nearby buckets
		// include more supporting cells
		float[] curPointCoor = ((Record) curPoint.getObj()).getValue();
		float[] ExtendArea = { Math.max(partition_store[0], curPointCoor[0] - kdist),
				Math.min(partition_store[1], curPointCoor[0] + kdist),
				Math.max(partition_store[2], curPointCoor[1] - kdist),
				Math.min(partition_store[3], curPointCoor[1] + kdist) };
		ArrayList<LargeCellStore> supportingLargeCells = searchSupportingLargeCells(ExtendArea, ptn,
				large_cell_store.get(xx));
		// System.out.println("old kdistance: " + kdist);
		for (LargeCellStore supportingCell : supportingLargeCells) {
			// System.out.println("size of large cell store: " +
			// supportingCell.getNumOfPoints());
			if (checkRange(ExtendArea, supportingCell.getCoordinates())) {
				// find a leaf to start
				if (supportingCell.breakIntoSmallCells) {
					prQuadLeaf tempLeaf = RangeQuery(supportingCell.getRootForPRTree(), ExtendArea);
					if (tempLeaf != null) {
						// then find KNNs within the large cell
						findKnnsWithinOneCell(pq, curPoint, tempLeaf, supportingCell, K);
						kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;

						float[] newExtendArea = { Math.max(partition_store[0], curPointCoor[0] - kdist),
								Math.min(partition_store[1], curPointCoor[0] + kdist),
								Math.max(partition_store[2], curPointCoor[1] - kdist),
								Math.min(partition_store[3], curPointCoor[1] + kdist) };
						ExtendArea = newExtendArea;
					}
				} // end if
				else { // traverse Large cell
					traverseLargeCell(pq, curPoint, supportingCell, K);
					kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;

					float[] newExtendArea = { Math.max(partition_store[0], curPointCoor[0] - kdist),
							Math.min(partition_store[1], curPointCoor[0] + kdist),
							Math.max(partition_store[2], curPointCoor[1] - kdist),
							Math.min(partition_store[3], curPointCoor[1] + kdist) };
					ExtendArea = newExtendArea;
				}
			} // end if(checkRange(ExtendArea, supportingCell.getCoordinates()))
		} // end for
			// System.out.println("new kdistance: " + kdist);
			// //bound supporting area for the partition
		savePriorityQueueToKNN(pq, curPoint, false, 0, K);
		float[] partitionExpand = boundPartitionSupport(TrueKnnPoints, curPoint, num_dims, partition_store, domains);
		// float partitionExpand = 0.0f;
		return partitionExpand;
	}

	public void findKnnsForOnePointSecondTime(MetricObject curPoint, prQuadLeaf curLeaf, LargeCellStore lcs,
			HashMap<Long, MetricObject> supportingPoints, float[] partition_store, int K, int num_dims,
			float[][] domains, Context context) {
		float kdist = Float.POSITIVE_INFINITY;
		PriorityQueue pq = new PriorityQueue(PriorityQueue.SORT_ORDER_DESCENDING);

		// first load in original knns if any
		if (curPoint.getKnnMoreDetail().size() != 0) {
			for (Map.Entry<Long, coreInfoKNNs> tempentry : curPoint.getKnnMoreDetail().entrySet()) {
				long curkey = tempentry.getKey();
				float curvalue = tempentry.getValue().dist;
				pq.insert(curkey, curvalue);
			}
		}
		kdist = ((pq.size() == K) ? pq.getPriority() : Float.POSITIVE_INFINITY);

		// first find kNNs within the large cell and bound a partition area for
		// largeCell
		// first find kNNs within the leaf
		pq = findKnns(pq, curPoint, curLeaf, K);
		kdist = ((pq.size() == K) ? pq.getPriority() : Float.POSITIVE_INFINITY);
		// then find KNNs within the large cell
		findKnnsWithinOneCell(pq, curPoint, curLeaf, lcs, K);
		kdist = ((pq.size() == K) ? pq.getPriority() : Float.POSITIVE_INFINITY);
		savePriorityQueueToKNNMore(pq, curPoint, supportingPoints, K, context);
	}

	public void findKnnsForOnePointInLargeCellSecondTime(MetricObject curPoint,
			HashMap<Long, MetricObject> supportingPoints, float[] partition_store, int K, int num_dims,
			float[][] domains, Context context) {
		float kdist = Float.POSITIVE_INFINITY;
		PriorityQueue pq = new PriorityQueue(PriorityQueue.SORT_ORDER_DESCENDING);

		// first load in original knns if any
		if (curPoint.getKnnMoreDetail().size() != 0) {
			for (Map.Entry<Long, coreInfoKNNs> tempentry : curPoint.getKnnMoreDetail().entrySet()) {
				long curkey = tempentry.getKey();
				float curvalue = tempentry.getValue().dist;
				pq.insert(curkey, curvalue);
			}
		}
		kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;

		// traverse the large cell
		float dist = 0.0f;
		float theta;
		if (pq.size() > 0)
			theta = pq.getPriority();
		else
			theta = Float.POSITIVE_INFINITY;
		for (MetricObject o_S : supportingPoints.values()) {
			if (((Record) o_S.getObj()).getRId() == ((Record) curPoint.getObj()).getRId()) {
				continue;
			} else if (o_S.getType() == 'C')
				continue;
			else {
				try {
					dist = metric.dist(curPoint.getObj(), o_S.getObj());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (pq.size() < K) {
					pq.insert(metricSpace.getID(o_S.getObj()), dist);
					theta = pq.getPriority();
				} else if (dist < theta) {
					pq.pop();
					pq.insert(metricSpace.getID(o_S.getObj()), dist);
					theta = pq.getPriority();
				}
			}
		}
		savePriorityQueueToKNNMore(pq, curPoint, supportingPoints, K, context);
	}

	public void savePriorityQueueToKNNMore(PriorityQueue pq, MetricObject curPoint,
			HashMap<Long, MetricObject> supportingPoints, int K, Context context) {
		Map<Long, coreInfoKNNs> orginalMap = new HashMap<Long, coreInfoKNNs>(curPoint.getKnnMoreDetail());
		curPoint.setKdist(pq.getPriority());
		curPoint.getKnnMoreDetail().clear();
		// if (pq.size() != K) {
		// context.getCounter(Counters.InputLessK).increment(1);
		// }
		// System.out.println("--------------------------------------------------");
		while (pq.size() > 0) {
			// System.out.println(pq.getValue() + "," + pq.getPriority());
			long tempId = pq.getValue();
			coreInfoKNNs tempInfo = null;
			if (orginalMap.containsKey(tempId))
				tempInfo = orginalMap.get(tempId);
			else if (supportingPoints.containsKey(tempId))
				tempInfo = new coreInfoKNNs(pq.getPriority(), supportingPoints.get(tempId).getKdist(),
						supportingPoints.get(tempId).getLrdValue());
			// else {
			// System.out.println("Can not find... Why...." + tempId);
			// }
			curPoint.getKnnMoreDetail().put(tempId, tempInfo);
			pq.pop();
		}
		// if(curPoint.getKnnMoreDetail().size() != K){
		// context.getCounter(Counters.OutputLessK).increment(1);
		// System.out.println("Less points second time.... " + pq.size());
		//
		// }
		curPoint.setOrgType('T');
	}

	public float findKnnsForLargeCell() {
		float partitionExpand = 0.0f;
		return partitionExpand;
	}

	public boolean isBreakIntoSmallCells() {
		return breakIntoSmallCells;
	}

	public void setBreakIntoSmallCells(boolean breakIntoSmallCells) {
		this.breakIntoSmallCells = breakIntoSmallCells;
	}

	/** print Large cell store */
	public String printCellStoreWithSupport() {
		String str = "";
		str += x_1 + "," + x_2 + "," + y_1 + "," + y_2 + "\n";
		str += "number of points:" + numOfPoints + "\n" + "closest pair distance: " + cpDist + "\n"
				+ "Points in detail: ";
		for (Iterator<MetricObject> itr = listOfPoints.iterator(); itr.hasNext();) {
			str = str + itr.next().getObj().toString() + "\n";

		}
		return str.substring(0, str.length());
	}

	public String printCellStoreDetailedInfo() {
		String str = "";
		str += x_1 + "," + x_2 + "," + y_1 + "," + y_2 + "\n";
		str += "number of points:" + numOfPoints + "\n" + "closest pair distance: " + cpDist + "\n";
		str += "isbreakup?" + breakIntoSmallCells + "\n";
		str += "numSmallCellsX  = " + numSmallCellsX + ",numSmallCellsY = " + numSmallCellsY + "\n";
		str += "small cell size = " + smallCellSize;
		return str.substring(0, str.length());
	}

	public float getCpDist() {
		return cpDist;
	}

	public void setCpDist(float cpDist) {
		this.cpDist = cpDist;
	}

	public float getSmallCellSize() {
		return smallCellSize;
	}

	public void setSmallCellSize(float smallCellSize) {
		this.smallCellSize = smallCellSize;
	}

	public IMetric getMetric() {
		return metric;
	}

	public void setMetric(IMetric metric) {
		this.metric = metric;
	}

	public int getNumOfPoints() {
		return numOfPoints;
	}

	public void setNumOfPoints(int numOfPoints) {
		this.numOfPoints = numOfPoints;
	}

	public float[] getCoordinates() {
		float[] Coordinates = { x_1, x_2, y_1, y_2 };
		return Coordinates;
	}

	public prQuadInternal getRootForPRTree() {
		return rootForPRTree;
	}

	public void setRootForPRTree(prQuadInternal rootForPRTree) {
		this.rootForPRTree = rootForPRTree;
	}

	public ArrayList<prQuadLeaf> getPrLeaves() {
		return prLeaves;
	}

	public void setPrLeaves(ArrayList<prQuadLeaf> prLeaves) {
		this.prLeaves = prLeaves;
	}

	public int getIndexForLeaveNodesList() {
		return indexForLeaveNodesList;
	}

	public void setIndexForLeaveNodesList(int indexForLeaveNodesList) {
		this.indexForLeaveNodesList = indexForLeaveNodesList;
	}

	public boolean isSafeArea() {
		return InsidesafeArea;
	}

	public void setSafeArea(boolean safeArea) {
		this.InsidesafeArea = safeArea;
	}
}
