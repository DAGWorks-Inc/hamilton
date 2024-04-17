import { BsFileEarmarkBreak } from "react-icons/bs";
import { IoEnter } from "react-icons/io5";
import {
  PiShareNetworkFill,
  PiFunction,
  PiDotsThreeCircleBold,
} from "react-icons/pi";
import { VscCircleFilled } from "react-icons/vsc";
import { extractTagFromNode, getPythonTypeIconFromNode } from "../../../utils";
import { DAGNode, TerminalVizNodeType } from "./types";
import { NodeTemplate } from "../../../state/api/friendlyApi";

export const nodeKey = (node: {
  nodeTemplate: NodeTemplate;
  dagIndex: number;
}) => {
  return `${node.nodeTemplate.name}-${node.dagIndex}`;
};

export const iconsForNodes = (nodes: DAGNode[]) => {
  const icons = new Set(
    nodes.flatMap((n) => {
      const iconsForNode = [];
      // if (n.userDefined) {
      //   iconsForNode.push(IoEnter);
      // }
      iconsForNode.push(getPythonTypeIconFromNode(n.nodeTemplate));
      return iconsForNode;
    })
  );
  return Array.from(icons);
};

export const iconForGroupSpecName = (groupSpecName: TerminalVizNodeType) => {
  if (groupSpecName === "module") {
    return BsFileEarmarkBreak;
  } else if (groupSpecName === "subdag") {
    return PiShareNetworkFill;
  } else if (groupSpecName === "function") {
    return PiFunction;
  } else if (groupSpecName === "input") {
    return IoEnter;
  } else if (groupSpecName === "dataQuality") {
    return PiDotsThreeCircleBold;
  }

  // } else if (groupSpecName === "subdag")
  // {
  // return BiNetworkChart;
  // }
  return VscCircleFilled;
};

export const getArtifactTypes = (nodes: DAGNode[]): Set<string> => {
  // TODO -- use the attributes to get the artifact types if we have them
  const artifactTypes = nodes
    .map(
      (n) =>
        extractTagFromNode(n.nodeTemplate, "hamilton.data_saver.sink") ||
        extractTagFromNode(n.nodeTemplate, "hamilton.data_loader.source")
    )
    .filter((n) => n !== undefined) as string[];

  return new Set(artifactTypes);
};
