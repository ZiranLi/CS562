from turicreate import*
import psutil
g = turicreate.load_sgraph('http://snap.stanford.edu/data/bigdata/communities/com-youtube.ungraph.txt.gz', format = 'snap')
pr = turicreate.pagerank.create(g)
print(pr.reset_probability)
print(pr.graph) # A new SGraph with the pagerank as a vertex property
print(pr.delta) # Total changes in pagerank during the last iteration (the L1 norm of the changes)
print(pr.pagerank) # An SFrame with each vertex’s pagerank
print(pr.num_iterations) # Number of iterations
print(pr.threshold) # The convergence threshold in L1 norm
print(pr.training_time)
print(pr.num_iterations)
print(psutil.cpu_percent())
print(psutil.virtual_memory())