// import ReactFlow, {
//   useNodesState,
//   useEdgesState,
//   Handle,
//   Position,
//   ReactFlowProvider,
// } from "reactflow";

import {
  DAGTemplateWithData,
  NodeTemplate,
} from "../../../state/api/friendlyApi";
import { VisualizeDAG } from "../Visualize/DAGViz";
import { VizType } from "../Visualize/types";

export const FunctionGraphView = (props: {
  upstreamNodes: NodeTemplate[];
  nodesProducedByFunction: NodeTemplate[];
  dagTemplate: DAGTemplateWithData;
}) => {
  const nodesWeCareAboutSet = new Set([
    ...props.upstreamNodes.map((node) => node.name),
    ...props.nodesProducedByFunction.map((node) => node.name),
  ]);
  const upstreamNodesSet = new Set(
    props.upstreamNodes.map((node) => node.name)
  );
  const modifiedDAGTemplate = {
    ...props.dagTemplate,
    nodes: props.dagTemplate.nodes
      .filter((node) => nodesWeCareAboutSet.has(node.name))
      .map((n) => {
        return {
          ...n,
          // This is not ideal -- we're adding new types that the API doesn't know about
          // That said its not terrible -- no reason we shouldn't be able to add arbitrary properties
          // -- as we're making the DAGViz one class for everything with different cases, I'm happy to pile on another case here
          classifications: upstreamNodesSet.has(n.name)
            ? ["external_to_subdag"]
            : n.classifications,
          // dependencies: Object.fromEntries(
          //   Object.entries(n.dependencies as string[]).filter(([k, v]) =>
          //     nodesWeCareAboutSet.has(k)
          //   )
          // ),
        };
      }),
  };
  return (
    <VisualizeDAG
      templates={[modifiedDAGTemplate]}
      runs={undefined}
      vizType={VizType.StaticDAG}
      enableLineageView={false}
      enableVizConsole={false}
      displayLegend={false}
      enableGrouping={false}
      height="h-[300px]"
      displayControls={true}
      vertical
    ></VisualizeDAG>
  );
};
