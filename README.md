# node-tainter

Node tainter works with NPD to taint nodes that have been flagged with a condition using NPD. This plugs the functionality between NPD and descheduler.

NPD -> adds conditions on unhealthy nodes
node-tainter -> taints nodes with conditions
descheduler -> removes tainted nodes
