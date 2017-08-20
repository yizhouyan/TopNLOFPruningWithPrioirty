package metricspace;

import java.util.HashMap;
import java.util.Map;

import util.PriorityQueue;

@SuppressWarnings("rawtypes")
public class MetricObjectMore{

	private int partition_id;
	private char type='F';
	// for second or more use
	private char orgType;
	private Object obj;
	private Map<Long, coreInfoKNNs> knnMoreDetail = new HashMap<Long, coreInfoKNNs>();
	private float kdist = -1;
	private float lrdValue = -1;
	private float lofValue = -1;
	private String whoseSupport="";
	private boolean canPrune = false;
	private int []indexForSmallCell;
	
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
	
	public MetricObjectMore() {
	}

	public MetricObjectMore (int partition_id, Object obj){
		this.partition_id = partition_id;
		this.obj = obj;
	}
	public MetricObjectMore (Object obj, char type, float lrd){
		this.obj = obj;
		this.type = type;
		this.lrdValue = lrd;
	}
	public MetricObjectMore (int partition_id, Object obj, char type){
		this(partition_id, obj);
		this.type = type;
	}
	public MetricObjectMore (int partition_id, Object obj, char type, char orgType, float kdist, float lrd, float lof){
		this(partition_id, obj);
		this.type = type;
		this.orgType = orgType;
		this.kdist = kdist;
		this.lrdValue = lrd;
		this.lofValue = lof;
	}
	public MetricObjectMore(int partition_id, Object obj, char curTag, char orgTag, Map<Long, coreInfoKNNs> knnInDetail, 
			float curKdist, float curLrd, float curLof, String whoseSupport){
		this(partition_id,  obj,  curTag,  orgTag, 
				 curKdist,  curLrd,  curLof);
		this.knnMoreDetail = knnInDetail;
		this.whoseSupport = whoseSupport;
	}

	public MetricObjectMore(Object obj, char curTag, char orgTag, Map<Long, coreInfoKNNs> knnInDetail, 
			 float curLrd, float curLof){
		this.obj = obj;
		this.type = curTag;
		this.orgType = orgTag;
		this.lrdValue = curLrd;
		this.lofValue = curLof;
		this.knnMoreDetail = knnInDetail;
		
	}
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

	public char getOrgType() {
		return orgType;
	}

	public void setOrgType(char orgType) {
		this.orgType = orgType;
	}

	public Map<Long, coreInfoKNNs> getKnnMoreDetail() {
		return knnMoreDetail;
	}

	public void setKnnMoreDetail(Map<Long, coreInfoKNNs> knnMoreDetail) {
		this.knnMoreDetail = knnMoreDetail;
	}

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

	public String getWhoseSupport() {
		return whoseSupport;
	}

	public void setWhoseSupport(String whoseSupport) {
		this.whoseSupport = whoseSupport;
	}

	public float getKdist() {
		return kdist;
	}

	public void setKdist(float kdist) {
		this.kdist = kdist;
	}
}