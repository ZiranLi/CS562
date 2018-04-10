from turicreate import*
import psutil
g = turicreate.load_sgraph('http://snap.stanford.edu/data/bigdata/communities/com-youtube.ungraph.txt.gz', format = 'snap')
tc = turicreate.triangle_counting.create(g)
print(tc);
print(tc.triangle_count); # An SFrame with each vertex’s id and triangle count
print(tc.graph); # A new SGraph with the triangle count as a vertex property
print(tc.training_time);
print(psutil.cpu_percent())
print(psutil.virtual_memory())