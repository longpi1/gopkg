package flow

import (
	"context"
	"fmt"
)

type Task interface {
	// NodeName 获取节点名称
	NodeName() string

	Run(ctx context.Context, data DataSet) error
}

// Node The vertex
type Node struct {
	Id       string // The id of the vertex
	index    int    // The index of the vertex
	uniqueId string // The unique Id of the node

	// Execution modes ([]operation / Dag)
	subDag          *Dag            // Subdag
	conditionalDags map[string]*Dag // Conditional subdags
	operations      []Operation     // The list of operations

	dynamic       bool                 // Denotes if the node is dynamic
	aggregator    Aggregator           // The aggregator aggregates multiple inputs to a node into one
	foreach       ForEach              // If specified foreach allows to execute the vertex in parallel
	condition     Condition            // If specified condition allows to execute only selected sub-flow
	subAggregator Aggregator           // Aggregates foreach/condition outputs into one
	forwarder     map[string]Forwarder // The forwarder handle forwarding output to a children

	task            Task
	parentDag       *Dag    // The reference of the flow this node part of
	indegree        int     // The vertex flow indegree
	dynamicIndegree int     // The vertex flow dynamic indegree
	outdegree       int     // The vertex flow outdegree
	children        []*Node // The children of the vertex
	dependsOn       []*Node // The parents of the vertex

	next []*Node
	prev []*Node
}

// inSlice check if a node belongs in a slice
func (node *Node) inSlice(list []*Node) bool {
	for _, b := range list {
		if b.Id == node.Id {
			return true
		}
	}
	return false
}

// Children get all children node for a node
func (node *Node) Children() []*Node {
	return node.children
}

// Dependency get all dependency node for a node
func (node *Node) Dependency() []*Node {
	return node.dependsOn
}

// Value provides the ordered list of functions for a node
func (node *Node) Operations() []Operation {
	return node.operations
}

// Indegree returns the no of input in a node
func (node *Node) Indegree() int {
	return node.indegree
}

// DynamicIndegree returns the no of dynamic input in a node
func (node *Node) DynamicIndegree() int {
	return node.dynamicIndegree
}

// Outdegree returns the no of output in a node
func (node *Node) Outdegree() int {
	return node.outdegree
}

// SubDag returns the subdag added in a node
func (node *Node) SubDag() *Dag {
	return node.subDag
}

// Dynamic checks if the node is dynamic
func (node *Node) Dynamic() bool {
	return node.dynamic
}

// ParentDag returns the parent flow of the node
func (node *Node) ParentDag() *Dag {
	return node.parentDag
}

// AddOperation adds an operation
func (node *Node) AddOperation(operation Operation) {
	node.operations = append(node.operations, operation)
}

// AddAggregator add a aggregator to a node
func (node *Node) AddAggregator(aggregator Aggregator) {
	node.aggregator = aggregator
}

// AddForEach add a aggregator to a node
func (node *Node) AddForEach(foreach ForEach) {
	node.foreach = foreach
	node.dynamic = true
	node.AddForwarder("dynamic", DefaultForwarder)
}

// AddCondition add a condition to a node
func (node *Node) AddCondition(condition Condition) {
	node.condition = condition
	node.dynamic = true
	node.AddForwarder("dynamic", DefaultForwarder)
}

// AddSubAggregator add a foreach aggregator to a node
func (node *Node) AddSubAggregator(aggregator Aggregator) {
	node.subAggregator = aggregator
}

// AddForwarder adds a forwarder for a specific children
func (node *Node) AddForwarder(children string, forwarder Forwarder) {
	node.forwarder[children] = forwarder
	if forwarder != nil {
		node.parentDag.dataForwarderCount = node.parentDag.dataForwarderCount + 1
		node.parentDag.executionFlow = false
	} else {
		node.parentDag.dataForwarderCount = node.parentDag.dataForwarderCount - 1
		if node.parentDag.dataForwarderCount == 0 {
			node.parentDag.executionFlow = true
		}
	}
}

// AddSubDag adds a subdag to the node
func (node *Node) AddSubDag(subDag *Dag) error {
	parentDag := node.parentDag
	// Continue till there is no parent flow
	for parentDag != nil {
		// check if recursive inclusion
		if parentDag == subDag {
			return ErrRecursiveDep
		}
		// Check if the parent flow is a subdag and has a parent node
		parentNode := parentDag.parentNode
		if parentNode != nil {
			// If a subdag, move to the parent flow
			parentDag = parentNode.parentDag
			continue
		}
		break
	}
	// Set the subdag in the node
	node.subDag = subDag
	// Set the node the subdag belongs to
	subDag.parentNode = node

	return nil
}

// AddForEachDag adds a foreach subdag to the node
func (node *Node) AddForEachDag(subDag *Dag) error {
	// Set the subdag in the node
	node.subDag = subDag
	// Set the node the subdag belongs to
	subDag.parentNode = node

	node.parentDag.hasBranch = true
	node.parentDag.hasEdge = true

	return nil
}

// AddConditionalDag adds conditional flow to node
func (node *Node) AddConditionalDag(condition string, dag *Dag) {
	// Set the conditional subdag in the node
	if node.conditionalDags == nil {
		node.conditionalDags = make(map[string]*Dag)
	}
	node.conditionalDags[condition] = dag
	// Set the node the subdag belongs to
	dag.parentNode = node

	node.parentDag.hasBranch = true
	node.parentDag.hasEdge = true
}

// GetAggregator get a aggregator from a node
func (node *Node) GetAggregator() Aggregator {
	return node.aggregator
}

// GetForwarder gets a forwarder for a children
func (node *Node) GetForwarder(children string) Forwarder {
	return node.forwarder[children]
}

// GetSubAggregator gets the subaggregator for condition and foreach
func (node *Node) GetSubAggregator() Aggregator {
	return node.subAggregator
}

// GetCondition get the condition function
func (node *Node) GetCondition() Condition {
	return node.condition
}

// GetForEach get the foreach function
func (node *Node) GetForEach() ForEach {
	return node.foreach
}

// GetAllConditionalDags get all the subdags for all conditions
func (node *Node) GetAllConditionalDags() map[string]*Dag {
	return node.conditionalDags
}

// GetConditionalDag get the sundag for a specific condition
func (node *Node) GetConditionalDag(condition string) *Dag {
	if node.conditionalDags == nil {
		return nil
	}
	return node.conditionalDags[condition]
}

// generateUniqueId returns a unique ID of node throughout the DAG
func (node *Node) generateUniqueId(dagId string) string {
	// Node Id : <flow-id>_<node_index_in_dag>_<node_id>
	return fmt.Sprintf("%s_%d_%s", dagId, node.index, node.Id)
}

// GetUniqueId returns a unique ID of the node
func (node *Node) GetUniqueId() string {
	return node.uniqueId
}
