package metricspace;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import metricspace.MetricKey;

public class PriorityComparator extends WritableComparator {
	protected PriorityComparator(){
		super(MetricKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2){
		MetricKey m1 = (MetricKey) w1;
		MetricKey m2 = (MetricKey) w2;
		
		int cmp;
		if(m1.priority > m2.priority)
			return -1;
		else if(m1.priority < m2.priority)
			return 1;
		else{
			if(m1.pid > m2.pid)
				return 1;
			else if(m1.pid < m2.pid)
				return -1;
			else
				return 0;
		}
	}
}
