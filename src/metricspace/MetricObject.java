package metricspace;

import java.util.HashMap;
import java.util.Map;

import lof.pruning.PriorityQueue;

@SuppressWarnings("rawtypes")
public class MetricObject{

	private int partition_id;
	private char type='F';
	// for second or more use
//	private char orgType;
	private Object obj;
//	private Map<Long,Float> knnInDetail = new HashMap<Long,Float>();
	public PriorityQueue pointPQ = new PriorityQueue(PriorityQueue.SORT_ORDER_DESCENDING);
//	private Map<Long, coreInfoKNNs> knnMoreDetail = new HashMap<Long, coreInfoKNNs>();
	private float kdist = -1;
	private float lrdValue = -1;
	private float lofValue = -1;
//	private String whoseSupport="";
//	private String knnsInString = "";
	private float nearestNeighborDist = Float.MAX_VALUE;
	private boolean canPrune = false;
	private int []indexForSmallCell;
	private int indexOfCPCellInList = -1;  // index of which cell it is in
	private float largeCellExpand = 0.0f;
	private boolean insideKNNfind = false;
	public float getNearestNeighborDist() {
		return nearestNeighborDist;
	}
	
	public void setNearestNeighborDist(float nearestNeighborDist) {
		this.nearestNeighborDist = nearestNeighborDist;
	}
	public float getLrdValue() {
		return lrdValue;
	}

	public void setLrdValue(float lrdValue) {
		this.lrdValue = lrdValue;
	}

	public float getLofValue() {
		return lofValue;
	}

	public void setLofValue(float lofValue) {
		this.lofValue = lofValue;
	}

	
	
	public int[] getIndexForSmallCell() {
		return indexForSmallCell;
	}

	public void setIndexForSmallCell(int[] indexForSmallCell) {
		this.indexForSmallCell = indexForSmallCell;
	}

	public boolean isCanPrune() {
		return canPrune;
	}

	public void setCanPrune(boolean canPrune) {
		this.canPrune = canPrune;
	}

//	public float getNearestNeighborDist() {
//		return nearestNeighborDist;
//	}
//
//	public void setNearestNeighborDist(float nearestNeighborDist) {
//		this.nearestNeighborDist = nearestNeighborDist;
//	}
	
	public MetricObject() {
	}

	public MetricObject (int partition_id, Object obj){
		this.partition_id = partition_id;
		this.obj = obj;
	}
	public MetricObject (Object obj, char type, float lrd){
		this.obj = obj;
		this.type = type;
		this.lrdValue = lrd;
	}
	public MetricObject (int partition_id, Object obj, char type){
		this(partition_id, obj);
		this.type = type;
	}
//	public MetricObject (int partition_id, Object obj, char type, char orgType, float kdist, float lrd, float lof){
//		this(partition_id, obj);
//		this.type = type;
//		this.orgType = orgType;
//		this.kdist = kdist;
//		this.lrdValue = lrd;
//		this.lofValue = lof;
//	}
//	public MetricObject(int partition_id, Object obj, char curTag, char orgTag, Map<Long, coreInfoKNNs> knnInDetail, 
//			float curKdist, float curLrd, float curLof, String whoseSupport){
//		this(partition_id,  obj,  curTag,  orgTag, 
//				 curKdist,  curLrd,  curLof);
//		this.knnMoreDetail = knnInDetail;
//		this.whoseSupport = whoseSupport;
//	}
//	public MetricObject(int partition_id, Object obj, float curKdist, Map<Long, Float> knnInDetail, 
//			char curTag, String whoseSupport){
//		this(partition_id,obj, curTag);
//		this.kdist = curKdist;
//		this.knnInDetail = knnInDetail;
//		this.whoseSupport = whoseSupport;
//	}
//	public MetricObject(int partition_id, Object obj, float curKdist, String knns, char curTag, String whoseSupport){
//		this(partition_id, obj, curTag);
//		this.kdist = curKdist;
//		this.knnsInString =  knns;
//		this.whoseSupport = whoseSupport;
//	}
//	public MetricObject(Object obj, char curTag, char orgTag, Map<Long, Float> knnInDetail, float curLrd, float curLof){
//		this.obj = obj;
//		this.type = curTag;
//		this.orgType = orgTag;
//		this.knnInDetail = knnInDetail;
//		this.lrdValue = curLrd;
//		this.lofValue = curLof;
//	}
	public String toString() {
		StringBuilder sb = new StringBuilder(); 
		Record r = (Record) obj;
		
		sb.append(",type: "+type);
		sb.append(",in Partition: "+partition_id);
		
		sb.append(", Knn in detail: ");
		
//		for (Long v : knnInDetail.keySet()) {
//			sb.append("," + v + "," + knnInDetail.get(v));
//		}
		return sb.toString();
	}

//	public char getOrgType() {
//		return orgType;
//	}
//
//	public void setOrgType(char orgType) {
//		this.orgType = orgType;
//	}

//	public Map<Long, coreInfoKNNs> getKnnMoreDetail() {
//		return knnMoreDetail;
//	}
//
//	public void setKnnMoreDetail(Map<Long, coreInfoKNNs> knnMoreDetail) {
//		this.knnMoreDetail = knnMoreDetail;
//	}

	public int getPartition_id() {
		return partition_id;
	}

	public void setPartition_id(int partition_id) {
		this.partition_id = partition_id;
	}

	public char getType() {
		return type;
	}

	public void setType(char type) {
		this.type = type;
	}

	public Object getObj() {
		return obj;
	}

	public void setObj(Object obj) {
		this.obj = obj;
	}

//	public String getWhoseSupport() {
//		return whoseSupport;
//	}
//
//	public void setWhoseSupport(String whoseSupport) {
//		this.whoseSupport = whoseSupport;
//	}

//	public Map<Long, Float> getKnnInDetail() {
//		return knnInDetail;
//	}
//
//	public void setKnnInDetail(Map<Long, Float> knnInDetail) {
//		this.knnInDetail = knnInDetail;
//	}

	public float getKdist() {
		return kdist;
	}

	public void setKdist(float kdist) {
		this.kdist = kdist;
	}
	
//	public float getExpandDist() {
//		return expandDist;
//	}
//
//	public void setExpandDist(float expandDist) {
//		this.expandDist = expandDist;
//	}
//	public String getKnnsInString() {
//		return knnsInString;
//	}
//
//	public void setKnnsInString(String knnsInString) {
//		this.knnsInString = knnsInString;
//	}
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		
	}

	public int getIndexOfCPCellInList() {
		return indexOfCPCellInList;
	}

	public void setIndexOfCPCellInList(int indexOfCPCellInList) {
		this.indexOfCPCellInList = indexOfCPCellInList;
	}

	public float getLargeCellExpand() {
		return largeCellExpand;
	}

	public void setLargeCellExpand(float largeCellExpand) {
		this.largeCellExpand = largeCellExpand;
	}

	public PriorityQueue getPointPQ() {
		return pointPQ;
	}

	public void setPointPQ(PriorityQueue pointPQ) {
		this.pointPQ = pointPQ;
	}

	public boolean isInsideKNNfind() {
		return insideKNNfind;
	}

	public void setInsideKNNfind(boolean insideKNNfind) {
		this.insideKNNfind = insideKNNfind;
	}
	
}