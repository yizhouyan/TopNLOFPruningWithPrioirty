package lof.pruning.firstknn;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.Reducer.Context;

import lof.pruning.firstknn.CalKdistanceFirstMultiDim.Counters;
import metricspace.MetricObject;

public class ComputeLOF {
	public static void CalLOFForSingleObject(Context context, MetricObject o_S, HashMap<Long, MetricObject> lrdHm,
			int K, float thresholdLof) throws IOException, InterruptedException {
		float lof_core = 0.0f;
		int countPruned = 0;
		if (o_S.getLrdValue() == 0)
			lof_core = 0;
		else {
			long[] KNN_moObjectsID = o_S.getPointPQ().getValueSet();

			for (int i = 0; i < KNN_moObjectsID.length; i++) {
				long temp_kNNKey = KNN_moObjectsID[i];

				if (!lrdHm.containsKey(temp_kNNKey)) {
					return;
				}
				float temp_lrd = lrdHm.get(temp_kNNKey).getLrdValue();
				if (temp_lrd == 0 || o_S.getLrdValue() == 0)
					continue;
				else
					lof_core += temp_lrd / o_S.getLrdValue() * 1.0f;
			}
			lof_core = lof_core / K * 1.0f;
		}
		if (Float.isNaN(lof_core) || Float.isInfinite(lof_core))
			lof_core = 0;
		o_S.setLofValue(lof_core);
		o_S.setType('O'); // calculated LOF
		// context.getCounter(Counters.CanCalLOFs).increment(1);
		if (lof_core <= thresholdLof) {
			// context.getCounter(Counters.ThirdPrune).increment(1);
			countPruned++;
			o_S.setCanPrune(true);
		}
		context.getCounter(Counters.FinalPrunedPoints).increment(countPruned);
	}
}
