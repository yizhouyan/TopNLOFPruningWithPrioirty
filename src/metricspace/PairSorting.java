package metricspace;

public class PairSorting implements Comparable<PairSorting> {
	public final int index;
	public final double value;

	public PairSorting(int index, double value) {
		this.index = index;
		this.value = value;
	}

	@Override
	public int compareTo(PairSorting other) {
		// multiplied to -1 as the author need descending sort order
		return -1 * Double.valueOf(this.value).compareTo(other.value);
	}
}