# TOP N LOF Pruning with Priority
Implement bucket sorting during the first KNN search phase

Change the whole framework in one partition
From:
Compute CP and divide into buckets (pruning at the same time) -> KNN search -> LRD computation -> LOF computation

TO:
Compute CP and divide into buckets (pruning) -> sort buckets -> For each bucket, do : Inner bucket KNN search, Inner bucket LRD, Inner bucket LOF -> Outer bucket KNN search -> LRD computation -> LOF computation

Aims to set up a large threshold ASAP!
