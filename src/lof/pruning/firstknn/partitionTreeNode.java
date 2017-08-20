package lof.pruning.firstknn;

public abstract class partitionTreeNode {
	public partitionTreeNode parentNode = null;
	public void setParentNode(partitionTreeNode parentNode){
		this.parentNode = parentNode;
	}
}
