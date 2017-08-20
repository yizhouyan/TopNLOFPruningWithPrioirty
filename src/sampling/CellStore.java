package sampling;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import util.SQConfig;

public class CellStore{
	public int cellStoreId;
	public int core_partition_id = Integer.MIN_VALUE;
	public Set<Integer> support_partition_id = new HashSet<Integer>();
	public CellStore(int cellStoreId){
		this.cellStoreId = cellStoreId;
	}

	public CellStore(int cellStoreId, int core_partition_id){
		this.cellStoreId = cellStoreId;
		this.core_partition_id = core_partition_id;
	}
	public CellStore(){
		
	}
	
	public String printCellStoreBasic(){
		String str = "";
		str += cellStoreId+",";
		str += "C:" + core_partition_id;
		return str;
	}
	public String printCellStoreWithSupport(){
		String str = "";
		str += cellStoreId+",";
		str += "C:" + core_partition_id+","+ "S:";
		for(Iterator itr = support_partition_id.iterator(); itr.hasNext();){ 
			int keyiter = (Integer) itr.next();
			str += keyiter+SQConfig.sepStrForIDDist;
		}
		return str.substring(0,str.length()-1);
	}
	public static int ComputeCellStoreId(int [] data, int dim, int numCellPerDim){
		if(data.length != dim)
			return -1;
		int cellId = 0;
		for(int i = 0; i< dim; i++){
			cellId = cellId + (int) (data[i] * Math.pow(numCellPerDim, i));
		}
		return cellId;
	}
	public static int ComputeCellStoreId(String dataString, int dim, int numCellPerDim){
		String [] data = dataString.split(",");
		if(data.length != dim)
			return -1;
		int cellId = 0;
		for(int i = 0; i< dim; i++){
			cellId = cellId + (int) (Float.parseFloat(data[i]) * Math.pow(numCellPerDim, i));
		}
		return cellId;
	}
	
	public static int ComputeCellStoreId(float [] data, int dim, int numCellPerDim, int smallRange){
		if(data.length != dim)
			return -1;
		int cellId = 0;
		for(int i = 0; i< dim; i++){
			int tempIndex = (int) Math.floor(data[i]/smallRange);
			cellId = cellId + (int) (tempIndex * Math.pow(numCellPerDim, i));
		}
		return cellId;
	}
	public int [] GenerateCellCoordinate(int cellIdNum, int dim, int numCellPerDim){
		int []cellCoorIndex = new int [dim];
		for(int i = dim-1 ; i >=0 ;i--){
			int divider = (int) Math.pow(numCellPerDim, i);
			int countPerDim = cellIdNum/divider;
			cellCoorIndex[i] = countPerDim;
			cellIdNum = cellIdNum - countPerDim * divider;
		}
		return cellCoorIndex;
	}
	public static void main(String[] args) throws Exception {
		CellStore cs = new CellStore();
		int [] secondData = cs.GenerateCellCoordinate(21, 5, 4);
		for(int i = 0 ;i < 5;i++){
			System.out.println(secondData[i]);
		}
	}
}