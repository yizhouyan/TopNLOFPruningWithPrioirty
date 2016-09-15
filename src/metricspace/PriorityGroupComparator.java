package metricspace;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PriorityGroupComparator extends WritableComparator {
	protected PriorityGroupComparator(){
		super(MetricKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable o1, WritableComparable o2){
		MetricKey m1 = (MetricKey) o1;
		MetricKey m2 = (MetricKey) o2;
		if(m1.pid > m2.pid)
			return 1;
		else if(m1.pid < m2.pid)
			return -1;
		else
			return 0;
	}
}
