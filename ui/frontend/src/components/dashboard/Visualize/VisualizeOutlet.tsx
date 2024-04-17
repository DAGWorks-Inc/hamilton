import { useData as useDashboardData } from "../Dashboard";
import { VisualizeDAG } from "./DAGViz";
import { VizType } from "./types";

export const VisualizeOutlet = () => {
  const { dagTemplates } = useDashboardData();
  return (
    <VisualizeDAG
      templates={dagTemplates}
      runs={undefined}
      vizType={VizType.StaticDAG}
      displayLegend
      enableGrouping
      enableLineageView
      enableVizConsole
      displayMiniMap
      displayControls
    />
  );
};
