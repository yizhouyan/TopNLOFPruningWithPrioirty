package lof.pruning;

import java.util.ArrayList;

public class partitionTreeInternal extends partitionTreeNode {
	private ArrayList<partitionTreeNode> childNodes;
	private float[] xyCoordinates;
	
	public partitionTreeInternal(float [] xyCoordinates){
		this.xyCoordinates = new float[4];
		this.xyCoordinates = xyCoordinates;
		childNodes = new ArrayList<partitionTreeNode>();
	}
	public void addNewChild(partitionTreeNode newChild){
		childNodes.add(newChild);
	}
	public ArrayList<partitionTreeNode> getChildNodes() {
		return childNodes;
	}
	public void setChildNodes(ArrayList<partitionTreeNode> childNodes) {
		this.childNodes = childNodes;
	}
	public float [] getCoordinates(){
		return xyCoordinates;
	}
	public String printQuadInternal(){
		String str = "";
		str = str + "Point Coordinates: ";
		str = str + xyCoordinates[0] + "," + xyCoordinates[1] + "," + 
				 xyCoordinates[2] + "," +  xyCoordinates[3] + ",";
		if(this.parentNode == null)
			str = str + "NULL ";
		else
			str = str+"Not empty";
		str = str + "Num of Childs: " + childNodes.size();
		return str;
	}
	
}
