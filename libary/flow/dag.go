package flow

import (
	"encoding/json"
	"fmt"
)

var (
	ErrNoVertex = fmt.Errorf("flow has no vertex set")
	// ErrCyclic denotes that flow has a cycle
	ErrCyclic = fmt.Errorf("flow has cyclic dependency")
	// ErrDuplicateEdge denotes that a flow edge is duplicate
	ErrDuplicateEdge = fmt.Errorf("edge redefined")
	// ErrDuplicateVertex denotes that a flow edge is duplicate
	ErrDuplicateVertex = fmt.Errorf("vertex redefined")
	// ErrMultipleStart denotes that a flow has more than one start point
	ErrMultipleStart = fmt.Errorf("only one start vertex is allowed")
	// ErrRecursiveDep denotes that flow has a recursive dependecy
	ErrRecursiveDep = fmt.Errorf("flow has recursive dependency")
	// DefaultForwarder Default forwarder
	DefaultForwarder = func(data []byte) []byte { return data }
)

// Aggregator definition for the data aggregator of nodes
type Aggregator func(map[string][]byte) ([]byte, error)

// Forwarder definition for the data forwarder of nodes
type Forwarder func([]byte) []byte

// ForEach definition for the foreach function
type ForEach func([]byte) map[string][]byte

// Condition definition for the condition function
type Condition func([]byte) []string

// Dag The whole flow
type Dag struct {
	Id    string
	nodes map[string]*Node // the nodes in a flow

	parentNode *Node // In case the flow is a sub flow the node reference

	initialNode *Node // The start of a valid flow
	endNode     *Node // The end of a valid flow
	hasBranch   bool  // denotes the flow or its subdag has a branch
	hasEdge     bool  // denotes the flow or its subdag has edge
	validated   bool  // denotes the flow has been validated

	executionFlow      bool // Flag to denote if none of the node forwards data
	dataForwarderCount int  // Count of nodes that forwards data

	nodeIndex int // NodeIndex
}

func (dag *Dag) InitDag(filePath string) (*Dag, error) {
	//flowConfig := conf.GetFlowConfig(filePath)
	//for _, flow := range flowConfig.Flows {
	//	flow.Name
	//}
	return dag, nil
}

// NewDag Creates a Dag
func NewDag() *Dag {
	dag := new(Dag)
	dag.nodes = make(map[string]*Node)
	dag.Id = "0"
	dag.executionFlow = true
	return dag
}

// Append appends another flow into an existing flow
// Its a way to define and reuse subdags
// append causes disconnected flow which must be linked with edge in order to execute
func (dag *Dag) Append(appendDag *Dag) error {
	for nodeId, node := range appendDag.nodes {
		_, duplicate := appendDag.nodes[nodeId]
		if duplicate {
			return ErrDuplicateVertex
		}
		// add the node
		dag.nodes[nodeId] = node
	}
	return nil
}

// AddVertex create a vertex with id and operations
func (dag *Dag) AddVertex(id string, operations []Operation) *Node {

	node := &Node{Id: id, operations: operations, index: dag.nodeIndex + 1}
	node.forwarder = make(map[string]Forwarder, 0)
	node.parentDag = dag
	dag.nodeIndex = dag.nodeIndex + 1
	dag.nodes[id] = node
	return node
}

// AddEdge add a directed edge as (from)->(to)
// If vertex doesn't exists creates them
func (dag *Dag) AddEdge(from, to string) error {
	fromNode := dag.nodes[from]
	if fromNode == nil {
		fromNode = dag.AddVertex(from, []Operation{})
	}
	toNode := dag.nodes[to]
	if toNode == nil {
		toNode = dag.AddVertex(to, []Operation{})
	}

	// CHeck if duplicate (TODO: Check if one way check is enough)
	if toNode.inSlice(fromNode.children) || fromNode.inSlice(toNode.dependsOn) {
		return ErrDuplicateEdge
	}

	// Check if cyclic dependency (TODO: Check if one way check if enough)
	if fromNode.inSlice(toNode.next) || toNode.inSlice(fromNode.prev) {
		return ErrCyclic
	}

	// Update references recursively
	fromNode.next = append(fromNode.next, toNode)
	fromNode.next = append(fromNode.next, toNode.next...)
	for _, b := range fromNode.prev {
		b.next = append(b.next, toNode)
		b.next = append(b.next, toNode.next...)
	}

	// Update references recursively
	toNode.prev = append(toNode.prev, fromNode)
	toNode.prev = append(toNode.prev, fromNode.prev...)
	for _, b := range toNode.next {
		b.prev = append(b.prev, fromNode)
		b.prev = append(b.prev, fromNode.prev...)
	}

	fromNode.children = append(fromNode.children, toNode)
	toNode.dependsOn = append(toNode.dependsOn, fromNode)
	toNode.indegree++
	if fromNode.Dynamic() {
		toNode.dynamicIndegree++
	}
	fromNode.outdegree++

	// Add default forwarder for from node
	fromNode.AddForwarder(to, DefaultForwarder)

	// set has branch property
	if toNode.indegree > 1 || fromNode.outdegree > 1 {
		dag.hasBranch = true
	}

	dag.hasEdge = true

	return nil
}

// GetNode get a node by Id
func (dag *Dag) GetNode(id string) *Node {
	return dag.nodes[id]
}

// GetParentNode returns parent node for a subdag
func (dag *Dag) GetParentNode() *Node {
	return dag.parentNode
}

// GetInitialNode gets the initial node
func (dag *Dag) GetInitialNode() *Node {
	return dag.initialNode
}

// GetEndNode gets the end node
func (dag *Dag) GetEndNode() *Node {
	return dag.endNode
}

// HasBranch check if flow or its sub-dags has branch
func (dag *Dag) HasBranch() bool {
	return dag.hasBranch
}

// HasEdge check if flow or its sub-dags has edge
func (dag *Dag) HasEdge() bool {
	return dag.hasEdge
}

// Validate validates a flow and all sub-flow as per faas-flow flow requirements
// A validated graph has only one initialNode and one EndNode set
// if a graph has more than one end-node, a separate end-node gets added
func (dag *Dag) Validate() error {
	initialNodeCount := 0
	var endNodes []*Node

	if dag.validated {
		return nil
	}

	if len(dag.nodes) == 0 {
		return ErrNoVertex
	}

	for _, b := range dag.nodes {
		b.uniqueId = b.generateUniqueId(dag.Id)
		if b.indegree == 0 {
			initialNodeCount = initialNodeCount + 1
			dag.initialNode = b
		}
		if b.outdegree == 0 {
			endNodes = append(endNodes, b)
		}
		if b.subDag != nil {
			if dag.Id != "0" {
				// Dag Id : <parent-flow-id>_<parent-node-unique-id>
				b.subDag.Id = fmt.Sprintf("%s_%d", dag.Id, b.index)
			} else {
				// Dag Id : <parent-node-unique-id>
				b.subDag.Id = fmt.Sprintf("%d", b.index)
			}

			err := b.subDag.Validate()
			if err != nil {
				return err
			}

			if b.subDag.hasBranch {
				dag.hasBranch = true
			}

			if b.subDag.hasEdge {
				dag.hasEdge = true
			}

			if !b.subDag.executionFlow {
				//  Subdag have data edge
				dag.executionFlow = false
			}
		}
		if b.dynamic && b.forwarder["dynamic"] != nil {
			dag.executionFlow = false
		}
		for condition, cdag := range b.conditionalDags {
			if dag.Id != "0" {
				// Dag Id : <parent-flow-id>_<parent-node-unique-id>_<condition_key>
				cdag.Id = fmt.Sprintf("%s_%d_%s", dag.Id, b.index, condition)
			} else {
				// Dag Id : <parent-node-unique-id>_<condition_key>
				cdag.Id = fmt.Sprintf("%d_%s", b.index, condition)
			}

			err := cdag.Validate()
			if err != nil {
				return err
			}

			if cdag.hasBranch {
				dag.hasBranch = true
			}

			if cdag.hasEdge {
				dag.hasEdge = true
			}

			if !cdag.executionFlow {
				// Subdag have data edge
				dag.executionFlow = false
			}
		}
	}

	if initialNodeCount > 1 {
		return fmt.Errorf("%v, flow: %s", ErrMultipleStart, dag.Id)
	}

	// If there is multiple ends add a virtual end node to combine them
	if len(endNodes) > 1 {
		endNodeId := fmt.Sprintf("end_%s", dag.Id)
		blank := &BlankOperation{}
		endNode := dag.AddVertex(endNodeId, []Operation{blank})
		for _, b := range endNodes {
			// Create a edge
			err := dag.AddEdge(b.Id, endNodeId)
			if err != nil {
				return err
			}
			// mark the edge as execution dependency
			b.AddForwarder(endNodeId, nil)
		}
		dag.endNode = endNode
	} else {
		dag.endNode = endNodes[0]
	}

	dag.validated = true

	return nil
}

// GetNodes returns a list of nodes (including subdags) belong to the flow
func (dag *Dag) GetNodes(dynamicOption string) []string {
	var nodes []string
	for _, b := range dag.nodes {
		nodeId := ""
		if dynamicOption == "" {
			nodeId = b.GetUniqueId()
		} else {
			nodeId = b.GetUniqueId() + "_" + dynamicOption
		}
		nodes = append(nodes, nodeId)
		// excludes the dynamic subdag
		if b.dynamic {
			continue
		}
		if b.subDag != nil {
			subDagNodes := b.subDag.GetNodes(dynamicOption)
			nodes = append(nodes, subDagNodes...)
		}
	}
	return nodes
}

// IsExecutionFlow check if a flow doesn't use intermediate data
func (dag *Dag) IsExecutionFlow() bool {
	return dag.executionFlow
}

// GetDefinitionJson generate DAG definition as a json
func (dag *Dag) GetDefinitionJson() ([]byte, error) {
	root := &DagExporter{}

	// Validate the flow
	root.IsValid = true
	err := dag.Validate()
	if err != nil {
		root.IsValid = false
		root.ValidationError = err.Error()
	}

	exportDag(root, dag)
	encoded, err := json.MarshalIndent(root, "", "    ")
	return encoded, err
}

// GetDefinition represent DAG definition with exporter
func (dag *Dag) GetDefinition() (*DagExporter, error) {
	root := &DagExporter{}

	// Validate the flow
	root.IsValid = true
	err := dag.Validate()
	if err != nil {
		root.IsValid = false
		root.ValidationError = err.Error()
	}

	exportDag(root, dag)
	return root, err
}
