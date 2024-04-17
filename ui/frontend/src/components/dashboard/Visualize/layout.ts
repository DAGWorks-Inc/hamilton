import { VizEdge, VizNode } from "./types";

import ELK, { ElkNode } from "elkjs/lib/elk.bundled.js";

const elk = new ELK();

const convertGraphFromElk = (
  root: ElkNode,
  nodeNameMap: Map<string, VizNode>
) => {
  const output = [] as VizNode[];
  const queue = [root];
  while (queue.length > 0) {
    const node = queue.shift() as ElkNode;
    const vizNode = nodeNameMap.get(node.id);
    if (vizNode !== undefined) {
      (vizNode.position = {
        x: node.x as number, // + PARENT_PADDING.left,
        y: node.y as number, // + PARENT_PADDING.top,
      }),
        (vizNode.data.dimensions = {
          width: node.width as number, // + PARENT_PADDING.left + PARENT_PADDING.right,
          height: node.height as number,
          // PARENT_PADDING.bottom +
          // PARENT_PADDING.top,
        });
      output.push(vizNode);
    }
    queue.push(...(node.children || []));
  }
  return output;
};

export const getLayoutedElements = (
  nodes: VizNode[],
  edges: VizEdge[],
  nodeDimensions: Map<string, { width: number; height: number }>,
  vertical: boolean
) => {
  // Organize every node by its parents
  const nodesByParent = new Map<string, VizNode[]>([["root", []]]);
  nodes.forEach((node) => {
    const parent = node.parentNode || "root";
    if (!nodesByParent.has(parent)) {
      nodesByParent.set(parent, []);
    }
    nodesByParent.get(parent)?.push(node);
  });

  const buildGraph = (rootNode: VizNode): ElkNode => {
    const children = nodesByParent.get(rootNode.id) || [];
    const subGraph = children.map((child) => buildGraph(child));
    // let { width: minWidth, height: minHeight } = nodeDimensions.get(
    //   rootNode.id
    // ) || { minWidth: 0, minHeight: 0 };
    // minWidth = minWidth || 0;
    // minHeight = minHeight || 0;
    const graph = {
      id: rootNode.id,
      children: subGraph,
      targetPosition: "right", // TODO -- use the direction above
      sourcePosition: "left",
      layoutOptions: {
        "elk.padding": "[top=40,left=25,bottom=25,right=25]",
        // "elk.hierarchyHandling": "INCLUDE_CHILDREN",
        // "elk.nodeSize.constraints": "MINIMUM_SIZE",
        // "elk.nodeSize.minimum": `(${minHeight-2},${minWidth-2})`,
      },
      // ? isHorizontal
      //   ? "right"
      //   : "bottom"
      // : "right",
      // width: 1000,
      // height: 100,
      width: rootNode
        ? nodeDimensions.get(rootNode.id)?.width || 10
        : undefined,
      height: rootNode
        ? nodeDimensions.get(rootNode.id)?.height || 10
        : undefined,
    };
    return graph;
  };

  const elkOptions = {
    // "nodeLabels.padding": "[top=10,left=10,bottom=10,right=10]",
    "elk.hierarchyHandling": "INCLUDE_CHILDREN",
    // "elk.padding" : "[top=25,left=25,bottom=25,right=25]",
    // "elk.spacing.individual": "true",
    // "spacing.nodeNodeBetweenLayers": "150",
    "elk.algorithm": "layered",
    // "org.eclipse.elk.layered.layering.strategy": "STRETCH_WIDfasefasefsTH",
    // "elk.spacing.nodeNode": "20",
    // "elk.direction": direction == "LR" ? "RIGHT" : "DOWN",
    // "elk.layered.spacing.nodeNodeBetweenLayers": "100",
    // "elk.spacing.nodeNode": "80",
    // // "elk.algorithm": "mrtree",
  };

  const nodeNameMap = new Map(nodes.map((node) => [node.id, node]));
  const vizEdgeNameMap = new Map(edges.map((edge) => [edge.id, edge]));

  const subGraph = (nodesByParent.get("root") || []).map((node) =>
    buildGraph(node)
  );
  const graph = {
    id: "root",
    layoutOptions: elkOptions,
    children: subGraph,
    width: 1000,
    height: 200,
    edges: edges.map((edge) => ({
      ...edge,
      sources: [edge.source],
      targets: [edge.target],
    })),
  };
  return elk
    .layout(graph, {
      layoutOptions: {
        "org.eclipse.elk.direction": vertical ? "DOWN" : "RIGHT",
      },
    })
    .then((layoutedGraph) => ({
      nodes: convertGraphFromElk(layoutedGraph, nodeNameMap),
      edges: (layoutedGraph.edges || []).map((edge) => {
        const edgeId = edge.id;
        const originalEdge = vizEdgeNameMap.get(edgeId) as VizEdge;
        return originalEdge;
      }) as VizEdge[],
    }))
    .catch(console.error);
};
