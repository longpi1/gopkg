package flow

import "context"

type Flow struct {
	dag       *Dag
	readyChan chan *Node
	data      DataSet
}

func NewFlow(dag *Dag) *Flow {
	return &Flow{
		dag:       dag,
		readyChan: make(chan *Node, len(dag.nodes)),
		data:      NewDataSet(),
	}
}

func (flow *Flow) Run(ctx context.Context) *Flow {
	// 遍历图的节点，寻找入度为0的父节点
	for _, node := range flow.dag.nodes {
		if node.indegree == 0 {
			flow.readyChan <- node
		}
	}
	// 执行就绪通道中的节点任务
	for nodeTask := range flow.readyChan {
		if nodeTask != nil {
			go func() {
				err := flow.RunNode(ctx, nodeTask)
				if err != nil {

				}
			}()
		}

	}
	return flow
}

func (flow *Flow) RunNode(ctx context.Context, node *Node) (err error) {
	defer func() {
		// todo 一些后置操作
		flow.RunNodeDone(ctx, node, err)
	}()
	err = node.task.Run(ctx, flow.data)
	return err
}

func (flow *Flow) RunNodeDone(ctx context.Context, node *Node, err error) {

}
