package metricspace;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


@SuppressWarnings("rawtypes")
public class MetricKey implements WritableComparable {

	/** Partition ID */
	public int pid;
	/** distance from objects to the pivot */
	public double priority;

	public MetricKey(int pid, double priority) {
		this.pid = pid;
		this.priority = priority;
	}
	public MetricKey() {
		pid = 0;
		priority = 0.5;
	}
	
	public void set(int pid, double priority) {
		this.pid = pid;
		this.priority = priority;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		pid = in.readInt();
		priority = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(pid);
		out.writeDouble(priority);
	}

	@Override
	public int hashCode() {
		return pid;
	}

	public String toString() {
		return  Integer.toString(pid) + "," + Double.toString(priority);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

//	@Override
//	public int compareTo(Object obj) {
//		MetricKey other = (MetricKey) obj;
//
//		if (this.pid > other.pid)
//			return 1;
//		else if (this.pid < other.pid)
//			return -1;
//
//		if (this.dist > other.dist)
//			return 1;
//		else if (this.dist < other.dist)
//			return -1;
//		else
//			return 0;
//	}
}
