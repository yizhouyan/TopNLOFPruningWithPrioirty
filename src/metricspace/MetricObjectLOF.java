package metricspace;

import java.util.HashMap;
import java.util.Map;

import util.PriorityQueue;

@SuppressWarnings("rawtypes")
public class MetricObjectLOF{

	private int partition_id;
	private char type='F';
	private char orgType;
	private Object obj;
	private Map<Long,Float> knnInDetail = new HashMap<Long,Float>();
	
	private float lrdValue = -1;
	private float lofValue = -1;

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
	
	public MetricObjectLOF() {
	}

	public MetricObjectLOF (Object obj, char type, float lrd){
		this.obj = obj;
		this.type = type;
		this.lrdValue = lrd;
	}
	
	public MetricObjectLOF(Object obj, char curTag, Map<Long, Float> knnInDetail, float curLrd){
		this.obj = obj;
		this.type = curTag;
		this.knnInDetail = knnInDetail;
		this.lrdValue = curLrd;
	}

	public char getOrgType() {
		return orgType;
	}

	public void setOrgType(char orgType) {
		this.orgType = orgType;
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

	public Map<Long, Float> getKnnInDetail() {
		return knnInDetail;
	}

	public void setKnnInDetail(Map<Long, Float> knnInDetail) {
		this.knnInDetail = knnInDetail;
	}
}