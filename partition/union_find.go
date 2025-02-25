package partition

import "blockEmulator/params"

type UnionFind struct {
	Parent         map[string]string // Parent address of each node
	Rank           map[string]int    // Rank of each node
	HasUnioned     map[string]bool   // Tracks whether each node has been unioned
	UnionCount     map[string]int    // Number of Union operations for each node
	NodeUnionCount map[string]int    // Number of times each node has participated in Union operations
	MaxUnion       int               // Maximum number of Union participations per node
}

func NewUnionFind() *UnionFind {
	return &UnionFind{
		Parent:         make(map[string]string),
		Rank:           make(map[string]int),
		HasUnioned:     make(map[string]bool),
		UnionCount:     make(map[string]int),
		NodeUnionCount: make(map[string]int),
		MaxUnion:       params.MaxUnion,
	}
}

// Find operation: Returns the root parent of a group
func (uf *UnionFind) Find(addr string) string {

	if _, exists := uf.Parent[addr]; !exists {
		return addr
	}

	// Path compression to update the parent
	if uf.Parent[addr] != addr {
		uf.Parent[addr] = uf.Find(uf.Parent[addr]) // Recursively update the parent
	}

	return uf.Parent[addr]
}

// Union operation: Merges two groups and returns the parent address
func (uf *UnionFind) Union(addr1, addr2 string) string {
	if _, exists := uf.Parent[addr1]; !exists {
		uf.Parent[addr1] = addr1
		uf.Rank[addr1] = 0
		uf.HasUnioned[addr1] = false
		uf.UnionCount[addr1] = 0
		uf.NodeUnionCount[addr1] = 0
	}
	if _, exists := uf.Parent[addr2]; !exists {
		uf.Parent[addr2] = addr2
		uf.Rank[addr2] = 0
		uf.HasUnioned[addr2] = false
		uf.UnionCount[addr2] = 0
		uf.NodeUnionCount[addr2] = 0
	}

	root1 := uf.Find(addr1)
	root2 := uf.Find(addr2)

	// Skip if they already belong to the same group
	if root1 == root2 {
		return root1
	}

	// Check if the node has reached its maximum participation count
	if uf.NodeUnionCount[root1] >= uf.MaxUnion || uf.NodeUnionCount[root2] >= uf.MaxUnion {
		// Do not merge, return the original parent
		return root1
	}

	// Calculate the total merge count
	totalUnionCount := uf.UnionCount[root1] + uf.UnionCount[root2] + 1

	var parentAddr string

	if uf.Rank[root1] < uf.Rank[root2] {
		uf.Parent[root1] = root2
		parentAddr = root2
		// Update merge count
		uf.UnionCount[root2] = totalUnionCount
		// Update node participation count
		uf.NodeUnionCount[root1]++
		uf.NodeUnionCount[root2]++
	} else {
		uf.Parent[root2] = root1
		if uf.Rank[root1] == uf.Rank[root2] {
			uf.Rank[root1]++
		}
		parentAddr = root1
		uf.UnionCount[root1] = totalUnionCount
		uf.NodeUnionCount[root1]++
		uf.NodeUnionCount[root2]++
	}

	// Mark both nodes as having undergone a Union operation
	uf.HasUnioned[addr1] = true
	uf.HasUnioned[addr2] = true

	return parentAddr
}

// Checks if a node has undergone a Union operation
func (uf *UnionFind) HasBeenUnioned(addr string) bool {
	if _, exists := uf.HasUnioned[addr]; exists {
		return uf.HasUnioned[addr]
	}
	return false // Uninitialized nodes are considered as not unioned
}

func (uf *UnionFind) IsConnected(addr1, addr2 string) bool {
	return uf.Find(addr1) == uf.Find(addr2)
}

func (uf *UnionFind) GetParentMap() map[string]Vertex {
	parentMap := make(map[string]Vertex)
	for addr := range uf.Parent {
		if uf.HasUnioned[addr] {
			parentMap[addr] = Vertex{Addr: uf.Find(addr)}
		}
	}
	return parentMap
}

func (uf *UnionFind) GetReverseParentMap() map[string][]string {
	reverseParentMap := make(map[string][]string)
	for addr := range uf.Parent {
		if uf.HasUnioned[addr] {
			parentAddr := uf.Find(addr)
			reverseParentMap[parentAddr] = append(reverseParentMap[parentAddr], addr)
		}
	}
	return reverseParentMap
}
