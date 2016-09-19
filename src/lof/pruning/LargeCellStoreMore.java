package lof.pruning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import org.apache.hadoop.mapreduce.Reducer.Context;
import lof.pruning.PriorityQueue;
import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricObject;
import metricspace.MetricObjectMore;
import metricspace.Record;
import metricspace.coreInfoKNNs;

public class LargeCellStoreMore extends partitionTreeNode {

	private float x_1;
	private float x_2;
	private float y_1;
	private float y_2;

	/** list of points in the Large grid cell */
	private ArrayList<MetricObjectMore> listOfPoints;

	/** Number of points inside the Large cell */
	private int numOfPoints;

	/** Break up into small cells */
	private float smallCellSize;

	private int numSmallCellsX = 0;

	private int numSmallCellsY = 0;

	private IMetric metric = null;

	private IMetricSpace metricSpace = null;

	private boolean breakIntoSmallCells = false;

	private prQuadInternalMore rootForPRTree;
	// save leaves that cannot be pruned
	ArrayList<prQuadLeafMore> prLeaves;

	public LargeCellStoreMore(float x_1, float x_2, float y_1, float y_2, IMetric metric, IMetricSpace metricspace) {
		this.x_1 = x_1;
		this.x_2 = x_2;
		this.y_1 = y_1;
		this.y_2 = y_2;
		listOfPoints = new ArrayList<MetricObjectMore>();
		numOfPoints = 0;
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

	public void addPoints(MetricObjectMore newpoint) {
		listOfPoints.add(newpoint);
		numOfPoints++;
	}

	public ArrayList<MetricObjectMore> getListOfPoints() {
		return listOfPoints;
	}

	public void seperateLargeNoPrune(int K) {
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

		for (MetricObjectMore mo : listOfPoints) {
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
				K);
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
	public void buildPRQuadTree(HashMap<Long, MetricObjectMore> CanPrunePoints, int numSmallCellsX, int numSmallCellsY,
			float smallCellSize, ArrayList<MetricObjectMore> listOfPoints, int numOfPoints, float[] largeCellCoor,
			int K) {
		// init root
		int[] indexRangeInSmallCell = { 0, numSmallCellsX - 1, 0, numSmallCellsY - 1 };
		rootForPRTree = new prQuadInternalMore(largeCellCoor, indexRangeInSmallCell, null, numSmallCellsX,
				numSmallCellsY, smallCellSize);
		Stack<prQuadInternalMore> prQuadTree = new Stack<prQuadInternalMore>();
		HashMap<prQuadInternalMore, ArrayList<MetricObjectMore>> mapQuadInternalWithPoints = new HashMap<prQuadInternalMore, ArrayList<MetricObjectMore>>();
		mapQuadInternalWithPoints.put(rootForPRTree, listOfPoints);
		// save leaves in the pr tree
		prLeaves = new ArrayList<prQuadLeafMore>();
		prQuadTree.push(rootForPRTree);
		int count = 0;
		while (!prQuadTree.empty()) {
			prQuadInternalMore curPRNode = prQuadTree.pop();
			curPRNode.generateChilden(CanPrunePoints, curPRNode, prQuadTree, mapQuadInternalWithPoints, prLeaves,
					largeCellCoor, numSmallCellsX, numSmallCellsY, K);
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

	public void findKnnsWithinOneCell(PriorityQueue pq, MetricObjectMore curPoint, prQuadLeafMore curLeaf,
			LargeCellStoreMore large_cell_store, int K) {
		float[] curPointCoor = ((Record) curPoint.getObj()).getValue();
		float kdist = pq.size() == K ? pq.getPriority() : Float.POSITIVE_INFINITY;
		Stack<prQuadNodeMore> checkWithinOneTree = new Stack<prQuadNodeMore>();
		prQuadNodeMore tempCheckBreakNode = curLeaf;
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
			for (prQuadNodeMore brother : ((prQuadInternalMore) tempCheckBreakNode.getParentNode()).getChildNodes()) {
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
				prQuadNodeMore tempNode = checkWithinOneTree.pop();
				if (tempNode.getClass().getName().endsWith("prQuadLeafMore")) {
					if (tempNode.equals(curLeaf)) {
						continue;
					}
					pq = findKnns(pq, curPoint, (prQuadLeafMore) tempNode, K);
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
					for (prQuadNodeMore children : ((prQuadInternalMore) tempNode).getChildNodes()) {
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

	
	public prQuadLeafMore findLeafWithSmallCellIndex(prQuadInternalMore prRoot, int xx, int yy) {
		Stack<prQuadInternalMore> prQuadTreeInternal = new Stack<prQuadInternalMore>();
		prQuadTreeInternal.push(prRoot);

		while (!prQuadTreeInternal.empty()) {
			prQuadInternalMore curPRNode = prQuadTreeInternal.pop();
			// traverse 4 childs and save to the stack if inside the expecting
			// range
			for (prQuadNodeMore tempNode : curPRNode.getChildNodes()) {
				if (tempNode.getIndexInSmallCell()[0] <= xx && xx <= tempNode.getIndexInSmallCell()[1]
						&& tempNode.getIndexInSmallCell()[2] <= yy && yy <= tempNode.getIndexInSmallCell()[3]) {
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
				if (tempNode.getClass().getName().endsWith("prQuadInternalMore")) {
					prQuadTreeInternal.push((prQuadInternal) tempNode);
				} else { // leaf
					return (prQuadLeaf) tempNode;
				}
			}
		}
		return null;
	}

	public float calExtendDistance(LargeCellStoreMore large_cell_store, MetricObject curPoint, float kdist) {
		float extendDist = 0.0f;
		float[] curPointCoor = ((Record) curPoint.getObj()).getValue();
		extendDist = (float) Math.max(extendDist, large_cell_store.x_1 - (curPointCoor[0] - kdist));
		extendDist = (float) Math.max(extendDist, (curPointCoor[0] + kdist) - large_cell_store.x_2);
		extendDist = (float) Math.max(extendDist, large_cell_store.y_1 - (curPointCoor[1] - kdist));
		extendDist = (float) Math.max(extendDist, (curPointCoor[1] + kdist) - large_cell_store.y_2);
		return extendDist;
	}

	public void findKnnsForOnePointSecondTime(MetricObjectMore curPoint, prQuadLeafMore curLeaf, LargeCellStoreMore lcs,
			HashMap<Long, MetricObjectMore> supportingPoints, float[] partition_store, int K, int num_dims,
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

		// first find kNNs within the leaf
		pq = findKnns(pq, curPoint, curLeaf, K);
		kdist = ((pq.size() == K) ? pq.getPriority() : Float.POSITIVE_INFINITY);
		// then find KNNs within the large cell
		findKnnsWithinOneCell(pq, curPoint, curLeaf, lcs, K);
		kdist = ((pq.size() == K) ? pq.getPriority() : Float.POSITIVE_INFINITY);
		savePriorityQueueToKNNMore(pq, curPoint, supportingPoints, K, context);
	}

	public void findKnnsForOnePointInLargeCellSecondTime(MetricObjectMore curPoint,
			HashMap<Long, MetricObjectMore> supportingPoints, float[] partition_store, int K, int num_dims,
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
		float[] Coordinates = { x_1, x_2, y_1, y_2 };
		return Coordinates;
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
