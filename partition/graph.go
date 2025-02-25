// 图的相关操作
package partition

// 图中的结点，即区块链网络中参与交易的账户
type Vertex struct {
	Addr string // 账户地址
	// 其他属性待补充
}

// 描述当前区块链交易集合的图
type Graph struct {
	VertexSet map[Vertex]bool     // 节点集合，其实是 set
	EdgeSet   map[Vertex][]Vertex // 记录节点与节点间是否存在交易，邻接表
	// lock      sync.RWMutex       //锁，但是每个储存节点各自存储一份图，不需要此
}

// 创建节点
func (v *Vertex) ConstructVertex(s string) {
	v.Addr = s
}

// 增加图中的点
func (g *Graph) AddVertex(v Vertex) {
	if g.VertexSet == nil {
		g.VertexSet = make(map[Vertex]bool)
	}
	g.VertexSet[v] = true
}

// 增加图中的边
func (g *Graph) AddEdge(u, v Vertex) {
	// 如果没有点，则增加边，权恒定为 1
	if _, ok := g.VertexSet[u]; !ok {
		g.AddVertex(u)
	}
	if _, ok := g.VertexSet[v]; !ok {
		g.AddVertex(v)
	}
	if g.EdgeSet == nil {
		g.EdgeSet = make(map[Vertex][]Vertex)
	}
	// 无向图，使用双向边
	g.EdgeSet[u] = append(g.EdgeSet[u], v)
	g.EdgeSet[v] = append(g.EdgeSet[v], u)
}

// Remove an edge from the graph
func (g *Graph) RemoveEdge(u, v Vertex) {
	// Remove v from u's adjacency list
	if neighbors, ok := g.EdgeSet[u]; ok {
		g.EdgeSet[u] = removeFromSlice(neighbors, v)
	}

	// Remove u from v's adjacency list (since it's an undirected graph)
	if neighbors, ok := g.EdgeSet[v]; ok {
		g.EdgeSet[v] = removeFromSlice(neighbors, u)
	}
}

// Helper function to remove a vertex from a slice of vertices
func removeFromSlice(slice []Vertex, vertex Vertex) []Vertex {
	var newSlice []Vertex
	for _, v := range slice {
		if v != vertex {
			newSlice = append(newSlice, v)
		}
	}
	return newSlice
}

func (g *Graph) transferEdgesAndRemove(source, mergedVertex Vertex) {
	if edges, exists := g.EdgeSet[source]; exists {
		var edgesToRemove []Vertex // Store edges to be removed after the loop
		for _, neighbor := range edges {
			g.AddEdge(mergedVertex, neighbor)               // Add the edge to the target vertex
			edgesToRemove = append(edgesToRemove, neighbor) // Mark for later removal
		}

		// Remove edges from the source vertex
		for _, neighbor := range edgesToRemove {
			g.RemoveEdge(source, neighbor)
		}

		// Remove the source vertex's edges
		delete(g.EdgeSet, source)
	}

	// Remove the source vertex from the VertexSet
	delete(g.VertexSet, source)
}

func (g *Graph) UpdateGraphForPartialMerge(u, mergedVertex Vertex) {
	g.RemoveEdge(u, mergedVertex)
	g.transferEdgesAndRemove(u, mergedVertex)
}

// 复制图
func (dst *Graph) CopyGraph(src Graph) {
	dst.VertexSet = make(map[Vertex]bool)
	for v := range src.VertexSet {
		dst.VertexSet[v] = true
	}
	if src.EdgeSet != nil {
		dst.EdgeSet = make(map[Vertex][]Vertex)
		for v := range src.VertexSet {
			dst.EdgeSet[v] = make([]Vertex, len(src.EdgeSet[v]))
			copy(dst.EdgeSet[v], src.EdgeSet[v])
		}
	}
}

// 输出图
func (g Graph) PrintGraph() {
	for v := range g.VertexSet {
		print(v.Addr, " ")
		print("edge:")
		for _, u := range g.EdgeSet[v] {
			print(" ", u.Addr, "\t")
		}
		println()
	}
	println()
}
