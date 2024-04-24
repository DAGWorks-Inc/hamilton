import React from "react";
import { DAGNode, VizType } from "../../Visualize/types";
import {
  DAGRunWithData,
  DAGTemplateWithData,
} from "../../../../state/api/friendlyApi";
import { DAGRunViewContext } from "../../Visualize/DAGRun";
import { VisualizeDAG } from "../../Visualize/DAGViz";

export const DAGRunView: React.FC<{
  run: DAGRunWithData;
  highlightedTasks: string[] | null;
  dagTemplate: DAGTemplateWithData;
  setHighlightedTasks: (tasks: string[] | null) => void;
  isHighlighted: boolean;
  // TODO -- merge with the waterfall chart so we don't duplicate logic
  setHighlighted: (highlighted: boolean) => void;
}> = (props) => {
  const nodeClickHook = (n: DAGNode[]) => {
    const domNode = document.getElementById(`#${n[0].name}`);
    domNode?.scrollIntoView({ behavior: "smooth", block: "center" });
  };

  return (
    <div
      className={` ${props.isHighlighted ? "bg-dwdarkblue/5" : ""}`}
      onMouseEnter={() => props.setHighlighted(true)}
      onMouseLeave={() => props.setHighlighted(false)}
    >
      <DAGRunViewContext.Provider
        value={{
          highlighedTasks: new Set(props.highlightedTasks ?? []),
        }}
      >
        <VisualizeDAG
          templates={[props.dagTemplate]}
          height="h-[500px]"
          enableVizConsole={false}
          enableLineageView={false}
          nodeInteractions={{
            onNodeGroupEnter: (nodes) => {
              props.setHighlightedTasks(nodes.map((n) => n.name));
            },
            onNodeGroupLeave: () => {
              props.setHighlightedTasks(null);
            },
            onNodeGroupClick: nodeClickHook,
          }}
          // TODO -- when we get the grouped node type
          defaultGroupedTypes={{ function: false }}
          runs={[props.run]}
          vizType={VizType.DAGRun}
          displayLegend={false}
          enableGrouping={false}
          displayControls={true}
        />
      </DAGRunViewContext.Provider>
    </div>
  );
};
