from turicreate import*
import psutil
g = turicreate.load_sgraph('http://snap.stanford.edu/data/bigdata/communities/com-youtube.ungraph.txt.gz', format='snap')
sp = turicreate.shortest_path.create(g, source_vid=1)
path = sp.get_path(vid = 11);
print(path)
print(sp.graph) # A new SGraph with the distance as a vertex property
print(sp.distance) # An SFrame with each vertex’s distance to the source vertex
print(sp.weight_field) # The edge field for weight
print(sp.summary)
print(psutil.cpu_percent())
print(psutil.virtual_memory())