package lof.pruning;

import java.util.ArrayList;

public class prQuadInternalMore extends prQuadNodeMore {
	
	private ArrayList<prQuadNodeMore> childNodes;
	private int numChilds;
	
	public prQuadInternalMore(float [] xyCoordinates,  int [] indexInSmallCell, prQuadNodeMore parentNode,
			int numSmallCellsX, int numSmallCellsY, float smallCellSize){
		super(xyCoordinates, indexInSmallCell, parentNode, numSmallCellsX, numSmallCellsY,smallCellSize);
		childNodes = new ArrayList<prQuadNodeMore>();
		numChilds = 0;
	}
	public void addNewChild(prQuadNodeMore newChild){
		childNodes.add(newChild);
		numChilds++;
	}
	public ArrayList<prQuadNodeMore> getChildNodes() {
		return childNodes;
	}
	public void setChildNodes(ArrayList<prQuadNodeMore> childNodes) {
		this.childNodes = childNodes;
	}
	public int getNumChilds() {
		return numChilds;
	}
	public void setNumChilds(int numChilds) {
		this.numChilds = numChilds;
	}
}
