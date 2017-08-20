package util;

public class SQConfig {
	/**============================ basic threshold ================ */
	/** metric space */
	public static final String strMetricSpace = "lof.metricspace.dataspace";  
	/** metric */
	public static final String strMetric = "lof.metricspace.metric";
	/** number of K */
	public static final String strK = "lof.threshold.K";
	/** number of dimensions */
	public static final String strDimExpression = "lof.vector.dim";
	/** number of independent dims*/
	public static final String strIndependentDim = "lof.independent.dim";
	/** dataset original path*/
	public static final String dataset = "lof.dataset.input.dir";   
	/** number of reducers */
	public static final String strNumOfReducers = "lof.reducer.count";
	
	/**============================= seperators ================ */
	/** seperator for items of every record in the index */
	public static final String sepStrForRecord = ",";
	public static final String sepStrForKeyValue = "\t";
	public static final String sepStrForIDDist = "|";
	public static final String sepSplitForIDDist = "\\|";
	
	/**============================= data driven sampling ================ */
	/** domain values */
	public static final String strDomainMin = "lof.sampling.domain.min";
	public static final String strDomainMax = "lof.sampling.domain.max";
	/** number of partitions */
	public static final String strNumOfPartitions = "lof.sampling.partition.count";
	/** number of small cells per dimension */
	public static final String strNumOfSmallCells = "lof.sampling.cells.count";
	/** sampling percentage 1% = 100 or 0.1% = 1000 */
	public static final String strSamplingPercentage = "lof.sampling.percentage";
	/** path for sampling*/
	public static final String strPartitionPlanOutput = "lof.sampling.partitionplan.output";
	public static final String strDimCorrelationOutput = "lof.sampling.dimcorrelation.output";
	public static final String strPartitionAjacencyOutput = "lof.sampling.partitionadj.output";
	public static final String strCellsOutput = "lof.sampling.cells.output";
	public static final String strSamplingOutput = "lof.sampling.output";
	public static final String strSafePruningZoneSegment = "lof.knnfind.segment";
	
	/**============================= knn find first round =================== */
	public static final String strKnnSummaryOutput = "lof.knnfind.output";
	public static final String strKdistanceOutput = "lof.knnfind.knn.output";
	public static final String strKnnPartitionPlan = "lof.knnfind.partitionplan.output";
	public static final String strKnnCellsOutput = "lof.knnfind.cells.output";
	public static final String strKnnNewCellsOutput = "lof.knnfind.cells.newoutput";
	public static final String strIndexFilePath = "lof.knnfind.cells.indexfile";
	public static final String strKnnFirstRoundTopN = "lof.knnfind.topnlof.output";
	public static final String strTopNFirstSummary = "lof.knnfind.topnlof.summary";
	public static final String strKdistFinalOutput = "lof.kdistance.output";
	public static final String strMaxLimitSupportingArea = "lof.max.limit.supporting";
	
	public static final String strLRDOutput = "lof.lrd.output";
	public static final String strLOFOutput = "lof.final.output";
	public static final String strTOPNLOFOutput = "lof.final.topn.output";
	
	/**============================pruning parameters ====================== */
	public static final String strLargeCellPerDim = "lof.pruning.numlargecell";
	public static final String strLOFThreshold = "lof.pruning.threshold";
	public static final String strLOFTopNThreshold = "lof.pruning.topn.threshold";
}
