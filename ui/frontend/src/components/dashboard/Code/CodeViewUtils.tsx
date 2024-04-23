import { NodeTemplate } from "../../../state/api/friendlyApi";
import { getPythonTypeIconFromNode } from "../../../utils";

export type NodeType = "input" | "intermediate" | "output";
const colors = {
  output: "bg-dwdarkblue",
  input: "bg-dwdarkblue/50",
  intermediate: "bg-dwdarkblue",
};

export const NodeView: React.FC<{
  node: NodeTemplate;
  type: NodeType;
}> = ({ node, type }) => {
  const color = colors[type];
  const Icon = getPythonTypeIconFromNode(node);
  return (
    <div
      className={`rounded-lg p-1 px-2  text-white text-sm
      items-center flex flex-row gap-2 ${color}`}
    >
      <Icon></Icon>
      {node.name}
    </div>
  );
};
