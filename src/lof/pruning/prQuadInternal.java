package lof.pruning;

import java.util.ArrayList;

public class prQuadInternal extends prQuadNode {
	
	private ArrayList<prQuadNode> childNodes;
	private int numChilds;
	
	public prQuadInternal(float [] xyCoordinates,  int [] indexInSmallCell, prQuadNode parentNode,
			int numSmallCellsX, int numSmallCellsY, float smallCellSize){
		super(xyCoordinates, indexInSmallCell, parentNode, numSmallCellsX, numSmallCellsY,smallCellSize);
		childNodes = new ArrayList<prQuadNode>();
		numChilds = 0;
	}
	public void addNewChild(prQuadNode newChild){
		childNodes.add(newChild);
		numChilds++;
	}
	public ArrayList<prQuadNode> getChildNodes() {
		return childNodes;
	}
	public void setChildNodes(ArrayList<prQuadNode> childNodes) {
		this.childNodes = childNodes;
	}
	public int getNumChilds() {
		return numChilds;
	}
	public void setNumChilds(int numChilds) {
		this.numChilds = numChilds;
	}
	public String printQuadInternal(){
		String str = super.printPRQuadNode();
		str = str + "Num of Childs: " + numChilds;
		return str;
	}
}
