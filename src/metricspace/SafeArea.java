package metricspace;

public class SafeArea {
	private float [] partitionSize;
	private float [] extendAbsSize;
	
	public SafeArea(float [] partitionSize){
		this.partitionSize = new float[4];
		this.partitionSize = partitionSize;
		this.extendAbsSize = new float[4];
	}
	public float [] getExtendAbsSize() {
		return extendAbsSize;
	}
	public void setExtendAbsSize(float [] extendAbsSize) {
		this.extendAbsSize = extendAbsSize;
	}
	public float [] getPartitionSize() {
		return partitionSize;
	}
	public void setPartitionSize(float [] partitionSize) {
		this.partitionSize = partitionSize;
	}
	
	
}
