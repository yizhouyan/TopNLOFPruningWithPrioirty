package lof.pruning.secondknn;

import java.util.ArrayList;
import metricspace.MetricObjectMore;

public class prQuadLeafMore extends prQuadNodeMore{
	private int numPoints;
	private ArrayList<MetricObjectMore> listOfPoints;
	public int getNumPoints() {
		return numPoints;
	}
	public void setNumPoints(int numPoints) {
		this.numPoints = numPoints;
	}
	public ArrayList<MetricObjectMore> getListOfPoints() {
		return listOfPoints;
	}
	public void setListOfPoints(ArrayList<MetricObjectMore> listOfPoints) {
		this.listOfPoints = listOfPoints;
	}
	public boolean isCanPrune() {
		return canPrune;
	}
	public void setCanPrune(boolean canPrune) {
		this.canPrune = canPrune;
	}
	private boolean canPrune = false;
	public prQuadLeafMore(float [] xyCoordinates,  int [] indexInSmallCell, prQuadNodeMore parentNode,
			int [] numSmallCells, float smallCellSize, 
			ArrayList<MetricObjectMore> listOfPoints, int numPoints){
		super(xyCoordinates, indexInSmallCell, parentNode, numSmallCells,smallCellSize);
		this.listOfPoints = listOfPoints;
		this.numPoints = numPoints;
		this.canPrune = canPrune;
	}
}
