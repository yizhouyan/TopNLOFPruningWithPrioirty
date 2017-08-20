package lof.pruning.secondknn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import org.apache.hadoop.mapreduce.Reducer.Context;

import lof.pruning.firstknn.partitionTreeNode;
import lof.pruning.firstknn.prQuadTree.prQuadInternal;
import lof.pruning.firstknn.prQuadTree.prQuadLeaf;
import lof.pruning.firstknn.prQuadTree.prQuadNode;
import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricObject;
import metricspace.MetricObjectMore;
import metricspace.Record;
import metricspace.coreInfoKNNs;
import util.PriorityQueue;

public class LargeCellStoreMore extends partitionTreeNode {

	/**
	 * Coordinates of the large cell, save independent dims, for example,
	 * dims[0,1,2,3], independentDims [2,3] then save coordinates for dims [2,3]
	 */
	private float[] coordinates;

	/** list of points in the Large grid cell */
	private ArrayList<MetricObjectMore> listOfPoints;

	/** Number of points inside the Large cell */
	private int numOfPoints;

	/** Break up into small cells */
	private float smallCellSize;

	/**
	 * Keep track of number of small cells per dim, only for independent dims
	 */
	private int[] numSmallCells;

	private IMetric metric = null;

	private IMetricSpace metricSpace = null;

	private boolean breakIntoSmallCells = false;

	private prQuadInternalMore rootForPRTree;
	// save leaves that cannot be pruned
	ArrayList<prQuadLeafMore> prLeaves;

	public LargeCellStoreMore(float[] coordinates, IMetric metric, IMetricSpace metricspace) {
		this.coordinates = coordinates.clone();
		listOfPoints = new ArrayList<MetricObjectMore>();
		numOfPoints = 0;
		this.metric = metric;
		this.metricSpace = metricspace;
	}

	public int[] getNumSmallCells() {
		return numSmallCells;
	}

	public void setNumSmallCells(int[] numSmallCells) {
		this.numSmallCells = numSmallCells.clone();
	}

	public void addPoints(MetricObjectMore newpoint) {
		listOfPoints.add(newpoint);
		numOfPoints++;
	}

	public ArrayList<MetricObjectMore> getListOfPoints() {
		return listOfPoints;
	}

	public float multiplyArray(float[] array) {
		float result = 1;
		for (float element : array) {
			result *= element;
		}
		return result;
	}

	public void seperateLargeNoPrune(int K, int[] independentDims, int[] correlatedDims) {
		float[] LargeCellRange = new float[independentDims.length];
		for (int i = 0; i < independentDims.length; i++) {
			LargeCellRange[i] = coordinates[i * 2 + 1] - coordinates[i * 2];
		}
		smallCellSize = (float) Math.pow(multiplyArray(LargeCellRange) * 4 * K / numOfPoints,
				1.0 / LargeCellRange.length);
		for (float LargeCellRangeTemp : LargeCellRange) {
			if (smallCellSize >= LargeCellRangeTemp)
				return;
		}

		numSmallCells = new int[independentDims.length];
		// calculate how many small cells for each partition per dimension
		for (int i = 0; i < LargeCellRange.length; i++) {
			numSmallCells[i] = (int) Math.ceil(LargeCellRange[i] / smallCellSize);
		}
		breakIntoSmallCells = true;

		for (MetricObjectMore mo : listOfPoints) {
			Record record = (Record) mo.getObj();
			int[] indexInSmall = new int[independentDims.length];
			for (int i = 0; i < independentDims.length; i++) {
				float tempValue = record.getValue()[independentDims[i]];
				indexInSmall[i] = (int) (Math.floor(tempValue - coordinates[2 * i])
						/ (smallCellSize + Float.MIN_VALUE));
			}
			mo.setIndexForSmallCell(indexInSmall);
		}

		// build up PR quadtree
		float[] largeCellCoor = coordinates.clone();
		buildPRQuadTree(numSmallCells, smallCellSize, listOfPoints, numOfPoints, largeCellCoor, independentDims,
				correlatedDims, K);
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
	public void buildPRQuadTree(int[] numSmallCells, float smallCellSize, ArrayList<MetricObjectMore> listOfPoints,
			int numOfPoints, float[] largeCellCoor, int[] independentDims, int[] correlatedDims, int K) {
		// init root
		int[] indexRangeInSmallCell = new int[numSmallCells.length * 2];
		for (int i = 0; i < numSmallCells.length; i++) {
			indexRangeInSmallCell[2 * i] = 0;
			indexRangeInSmallCell[2 * i + 1] = numSmallCells[i] - 1;
		}
		rootForPRTree = new prQuadInternalMore(largeCellCoor, indexRangeInSmallCell, null, numSmallCells,
				smallCellSize);
		Stack<prQuadInternalMore> prQuadTree = new Stack<prQuadInternalMore>();
		HashMap<prQuadInternalMore, ArrayList<MetricObjectMore>> mapQuadInternalWithPoints = new HashMap<prQuadInternalMore, ArrayList<MetricObjectMore>>();
		mapQuadInternalWithPoints.put(rootForPRTree, listOfPoints);
		// save leaves in the pr tree
		prLeaves = new ArrayList<prQuadLeafMore>();
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
			prQuadInternalMore curPRNode = prQuadTree.pop();
			curPRNode.generateChilden(curPRNode, prQuadTree, mapQuadInternalWithPoints, prLeaves, largeCellCoor,
					numSmallCells, independentDims, correlatedDims, K);
		}
	}

	public PriorityQueue findKnns(PriorityQueue pq, MetricObjectMore curPoint, prQuadLeafMore curCheckLeaf, int K) {
		// traverse points
		float dist = 0.0f;
		float theta;
		if (pq.size() > 0)
			theta = pq.getPriority();
		else
			theta = Float.POSITIVE_INFINITY;
		for (int i = 0; i < curCheckLeaf.getNumPoints(); i++) {
			MetricObjectMore o_S = curCheckLeaf.getListOfPoints().get(i);
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
		return pq;
	}

	static boolean checkRange(float[] expectedRange, float[] checkedRange) {
		for (int i = 0; i < expectedRange.length / 2; i++) {
			if (expectedRange[2 * i] > checkedRange[2 * i + 1] || checkedRange[2 * i] > expectedRange[2 * i + 1])
				return false;
		}
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
		for (int i = 0; i < checkedArea.length / 2; i++) {
			if (extendedArea[2 * i] < checkedArea[2 * i] || extendedArea[2 * i + 1] > checkedArea[2 * i + 1])
				return false;
		}
		return true;
	}

	public void findKnnsWithinOneCell(PriorityQueue pq, MetricObjectMore curPoint, prQuadLeafMore curLeaf,
			LargeCellStoreMore large_cell_store, int K, int[] independentDims) {
		float[] curPointCoor = ((Record) curPoint.getObj()).getValue();
		float kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;
		Stack<prQuadNodeMore> checkWithinOneTree = new Stack<prQuadNodeMore>();
		prQuadNodeMore tempCheckBreakNode = curLeaf;
		float[] extendArea = new float[independentDims.length * 2];
		float[] largeCellCoor = large_cell_store.coordinates;
		for (int i = 0; i < independentDims.length; i++) {
			extendArea[2 * i] = Math.max(largeCellCoor[2 * i], curPointCoor[independentDims[i]] - kdist);
			extendArea[2 * i + 1] = Math.min(largeCellCoor[2 * i + 1], curPointCoor[independentDims[i]] + kdist);
		}
		float[] checkedCoordinates = tempCheckBreakNode.getCoordinates();
		if (insideCheckedArea(checkedCoordinates, extendArea)) {
			return;
		}
		while (tempCheckBreakNode.getParentNode() != null) {
			// first add the parent node
			checkWithinOneTree.push(tempCheckBreakNode.getParentNode());
			// System.out.println("TempCheckBreakNode: "+
			// tempCheckBreakNode.printPRQuadNode());
			// then add brothers
			for (prQuadNodeMore brother : ((prQuadInternalMore) tempCheckBreakNode.getParentNode()).getChildNodes()) {
				// System.out.println("Brother: "+ brother.printPRQuadNode());
				if (brother.equals(tempCheckBreakNode)) /////////
					continue;
				else if (checkRange(extendArea, brother.getCoordinates())) {
					checkWithinOneTree.push(brother);
				} else
					continue;
			}
			// traverse the stack until has only one element(the parent) left,
			// all brother traversed
			while (checkWithinOneTree.size() > 1) {
				prQuadNodeMore tempNode = checkWithinOneTree.pop();
				if (tempNode.getClass().getName().endsWith("prQuadLeafMore")) {
					if (tempNode.equals(curLeaf)) {
						continue;
					}
					pq = findKnns(pq, curPoint, (prQuadLeafMore) tempNode, K);
					kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;
					// update extend area
					float[] newExtendArea = new float[independentDims.length * 2];
					for (int i = 0; i < independentDims.length; i++) {
						newExtendArea[2 * i] = Math.max(largeCellCoor[2 * i], curPointCoor[independentDims[i]] - kdist);
						newExtendArea[2 * i + 1] = Math.min(largeCellCoor[2 * i + 1],
								curPointCoor[independentDims[i]] + kdist);
					}
					extendArea = newExtendArea;
					if (insideCheckedArea(checkedCoordinates, extendArea)) {
						return;
					}
				} else {
					// add children
					for (prQuadNodeMore children : ((prQuadInternalMore) tempNode).getChildNodes()) {
						if (children.equals(curLeaf))
							continue;
						else if (checkRange(extendArea, children.getCoordinates())) {
							checkWithinOneTree.push(children);
						} else
							continue;
					}
				} // end else
			} // end while
				// stack size == 1 only one parent left, all brothers traversed
			tempCheckBreakNode = checkWithinOneTree.pop(); // let parent be the
															// new node
			checkedCoordinates = tempCheckBreakNode.getCoordinates();
			if (insideCheckedArea(checkedCoordinates, extendArea)) {
				return;
			}
		}// end while
	}

	public prQuadLeafMore findLeafWithSmallCellIndex(prQuadInternalMore prRoot, int[] indexForSmallCells,
			int[] independentDims) {
		Stack<prQuadInternalMore> prQuadTreeInternal = new Stack<prQuadInternalMore>();
		prQuadTreeInternal.push(prRoot);

		while (!prQuadTreeInternal.empty()) {
			prQuadInternalMore curPRNode = prQuadTreeInternal.pop();
			// traverse 4 childs and save to the stack if inside the expecting
			// range
			for (prQuadNodeMore tempNode : curPRNode.getChildNodes()) {
				boolean tempFlag = true;
				int[] indexRangeOfSmallCell = tempNode.getIndexInSmallCell();
				for (int i = 0; i < independentDims.length; i++) {
					if (indexRangeOfSmallCell[2 * i] > indexForSmallCells[i]
							|| indexForSmallCells[i] > indexRangeOfSmallCell[2 * i + 1]) {
						tempFlag = false;
						break;
					}
				}
				if (tempFlag) {
					if (tempNode.getClass().getName().endsWith("prQuadInternalMore")) {
						prQuadTreeInternal.push((prQuadInternalMore) tempNode);
					} else { // leaf
						return (prQuadLeafMore) tempNode;
					}
				} else
					continue;
			}
		}
		return null;
	}

//	public prQuadLeaf RangeQuery(prQuadInternal prRoot, float[] expectedRange) {
//		Stack<prQuadInternal> prQuadTreeInternal = new Stack<prQuadInternal>();
//		prQuadTreeInternal.push(prRoot);
//		while (!prQuadTreeInternal.empty()) {
//			prQuadInternal curPRNode = prQuadTreeInternal.pop();
//			// traverse 4 childs and save to the stack if inside the expecting
//			// range
//			for (prQuadNode tempNode : curPRNode.getChildNodes()) {
//				// check range
//				if (!checkRange(expectedRange, tempNode.getXyCoordinates()))
//					continue;
//				if (tempNode.getClass().getName().endsWith("prQuadInternalMore")) {
//					prQuadTreeInternal.push((prQuadInternal) tempNode);
//				} else { // leaf
//					return (prQuadLeaf) tempNode;
//				}
//			}
//		}
//		return null;
//	}

	public void findKnnsForOnePointSecondTime(MetricObjectMore curPoint, prQuadLeafMore curLeaf, LargeCellStoreMore lcs,
			HashMap<Long, MetricObjectMore> supportingPoints, float[] partition_store, int K, int num_dims,
			float[] domains, int[] independentDims, Context context) {
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

		// first find kNNs within the leaf
		pq = findKnns(pq, curPoint, curLeaf, K);
		kdist = ((pq.size() == K) ? pq.getPriority() : Float.POSITIVE_INFINITY);
		// then find KNNs within the large cell
		findKnnsWithinOneCell(pq, curPoint, curLeaf, lcs, K, independentDims);
		kdist = ((pq.size() == K) ? pq.getPriority() : Float.POSITIVE_INFINITY);
		savePriorityQueueToKNNMore(pq, curPoint, supportingPoints, K, context);
	}

	public void findKnnsForOnePointInLargeCellSecondTime(MetricObjectMore curPoint,
			HashMap<Long, MetricObjectMore> supportingPoints, float[] partition_store, int K, int num_dims,
			float[] domains, int[] independentDims, Context context) {
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
		for (MetricObjectMore o_S : supportingPoints.values()) {
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

	public void savePriorityQueueToKNNMore(PriorityQueue pq, MetricObjectMore curPoint,
			HashMap<Long, MetricObjectMore> supportingPoints, int K, Context context) {
		Map<Long, coreInfoKNNs> orginalMap = new HashMap<Long, coreInfoKNNs>(curPoint.getKnnMoreDetail());
		curPoint.setKdist(pq.getPriority());
		curPoint.getKnnMoreDetail().clear();

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
		return this.coordinates;
	}

	public prQuadInternalMore getRootForPRTree() {
		return rootForPRTree;
	}

	public void setRootForPRTree(prQuadInternalMore rootForPRTree) {
		this.rootForPRTree = rootForPRTree;
	}

	public ArrayList<prQuadLeafMore> getPrLeaves() {
		return prLeaves;
	}

	public void setPrLeaves(ArrayList<prQuadLeafMore> prLeaves) {
		this.prLeaves = prLeaves;
	}
}
