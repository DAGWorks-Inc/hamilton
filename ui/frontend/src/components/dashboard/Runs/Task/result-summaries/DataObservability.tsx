import { useState } from "react";
import {
  AttributeDagworksDescribe3,
  AttributeDict1,
  AttributeDict2,
  AttributePandasDescribe1,
  AttributePrimitive1,
  AttributeUnsupported1,
} from "../../../../../state/api/backendApiRaw";
import {
  NodeRunWithAttributes,
  getNodeRunAttributes,
} from "../../../../../state/api/friendlyApi";
import { parsePythonType } from "../../../../../utils";
import { GenericTable } from "../../../../common/GenericTable";
import { RunLink } from "../../../../common/CommonLinks";
import { PandasDescribe1View } from "./PandasDescribe";
import { Dict1View, Dict2View } from "./DictView";
import { DAGWorksDescribe3View } from "./DAGWorksDescribe";

const Primitive1View = (props: {
  taskName: string;
  runIds: number[];
  values: AttributePrimitive1[];
  projectId: number;
}) => {
  return (
    <div className="m-8">
      <GenericTable
        data={props.values.map((item, i) => {
          return [props.runIds[i].toString() || "", item];
        })}
        columns={[
          {
            displayName: "type",
            Render: (item: AttributePrimitive1) => {
              return (
                <div className="flex flex-col">
                  <code className="text-sm">
                    {parsePythonType({ type_name: item.type })}
                  </code>
                </div>
              );
            },
          },
          {
            displayName: "value",
            Render: (item: AttributePrimitive1) => {
              const [expanded, setExpanded] = useState(false);
              return (
                <div className="flex flex-col w-144">
                  <pre
                    onClick={(e) => {
                      setExpanded(!expanded);
                      e.stopPropagation();
                    }}
                    className={`${
                      expanded ? "break-word whitespace-pre-wrap" : "truncate"
                    }  text-gray-500 cursor-cell`}
                  >
                    {item.value.toString()}
                  </pre>
                  {/* <code className="text-sm whitespace-pre-wrap">
                    {item.value.toString()}
                  </code> */}
                </div>
              );
            },
          },
        ]}
        dataTypeName={"Run"}
        dataTypeDisplay={(item: string) => {
          return (
            <RunLink
              projectId={props.projectId}
              runId={parseInt(item) as number}
              setHighlightedRun={() => void 0}
              highlightedRun={null}
            ></RunLink>
          );
        }}
      />
    </div>
  );
};

const Unsupported1View = (props: {
  taskName: string;
  runIds: number[];
  values: AttributeUnsupported1[];
}) => {
  return (
    <div className="flex flex-col m-20">
      <span className="text-lg text-gray-800">
        We currently do not capture data summaries for run(s){" "}
        <code>[{props.runIds.join(", ")}]</code>
        for <code>{props.taskName}</code>. We are working on adding support for
        everything -- reach out if you need it and we can prioritize!
      </span>
      <div className="m-8">
        <GenericTable
          data={props.values.map((item, i) => {
            return [props.runIds[i].toString() || "", item];
          })}
          columns={[
            {
              displayName: "type",
              Render: (item: AttributeUnsupported1) => {
                return (
                  <div className="flex flex-col">
                    <code className="text-sm">{item.unsupported_type}</code>
                  </div>
                );
              },
            },
            {
              displayName: "action",
              Render: (item: AttributeUnsupported1) => {
                return (
                  <div className="flex flex-col">
                    <span className="text-sm">{item.action}</span>
                  </div>
                );
              },
            },
          ]}
          dataTypeName={""}
        />
      </div>
    </div>
  );
};

export const ResultsSummaryView = (props: {
  nodeRunData: (NodeRunWithAttributes | null)[];
  taskName: string | undefined;
  projectId: number;
  runIds: number[];
}) => {
  const allViews = [];
  /**
   * We have to do this all manually (I think) due to type erasure -- not known at runtime
   * so we can't really do it in a for loop. I'm sure there's a way to but generating this
   * is quick enough.
   *
   * Thanks to chatGPT I could check my email while this code was getting written.
   *
   */

  const primitive1Views = getNodeRunAttributes<AttributePrimitive1>(
    props.nodeRunData.flatMap((i) => i?.attributes || []),
    props.runIds,
    "AttributePrimitive1"
  );
  if (primitive1Views.length > 0) {
    allViews.push(
      <Primitive1View
        taskName={props.taskName || ""}
        runIds={primitive1Views.map((i) => i.runId)}
        values={primitive1Views.map((i) => i.value)}
        projectId={props.projectId}
      />
    );
  }


  // ... [similar blocks for other attributes]

  // const error1Views = getNodeRunAttributes<AttributeError1>(
  //   props.nodeRunData.flatMap((i) => i?.attributes || []),
  //   "AttributeError1"
  // );
  // if (error1Views.length > 0) {
  //   allViews.push(
  //     <Error1View
  //       taskName={props.taskName || ""}
  //       runIds={error1Views.map((i) => i.runId)}
  //       values={error1Views.map((i) => i.value)}
  //     />
  //   );
  // }

  const dict1Views = getNodeRunAttributes<AttributeDict1>(
    props.nodeRunData.flatMap((i) => i?.attributes || []),
    props.runIds,
    "AttributeDict1"
  );

  if (dict1Views.length > 0) {
    allViews.push(
      <Dict1View
        taskName={props.taskName || ""}
        runIds={dict1Views.map((i) => i.runId)}
        values={dict1Views.map((i) => i.value)}
      />
    );
  }

  const dict2Views = getNodeRunAttributes<AttributeDict2>(
    props.nodeRunData.flatMap((i) => i?.attributes || []),
    props.runIds,
    "AttributeDict2"
  );
  if (dict2Views.length > 0) {
    allViews.push(
      <Dict2View
        taskName={props.taskName || ""}
        runIds={dict2Views.map((i) => i.runId)}
        values={dict2Views.map((i) => i.value)}
      />
    );
  }

  const dagworksDescribe3Views =
    getNodeRunAttributes<AttributeDagworksDescribe3>(
      props.nodeRunData.flatMap((i) => i?.attributes || []),
      props.runIds,
      "AttributeDagworksDescribe3"
    );
  if (dagworksDescribe3Views.length > 0) {
    allViews.push(
      <DAGWorksDescribe3View
        taskName={props.taskName || ""}
        runIds={dagworksDescribe3Views.map((i) => i.runId)}
        values={dagworksDescribe3Views.map((i) => i.value)}
        projectId={props.projectId}
      />
    );
  }

  const pandasDescribe1View = getNodeRunAttributes<AttributePandasDescribe1>(
    props.nodeRunData.flatMap((i) => i?.attributes || []),
    props.runIds,
    "AttributePandasDescribe1"
  );

  if (pandasDescribe1View.length > 0) {
    allViews.push(
      <PandasDescribe1View
        taskName={props.taskName || ""}
        runIds={pandasDescribe1View.map((i) => i.runId)}
        values={pandasDescribe1View.map((i) => i.value)}
        projectId={props.projectId}
      />
    );
  }

  const unsupportedViews = getNodeRunAttributes<AttributeUnsupported1>(
    props.nodeRunData.flatMap((i) => i?.attributes || []),
    props.runIds,
    "AttributeUnsupported1"
  );

  if (unsupportedViews.length > 0) {
    allViews.push(
      <Unsupported1View
        taskName={props.taskName || ""}
        runIds={unsupportedViews.map((i) => i.runId)}
        values={unsupportedViews.map((i) => i.value)}
      />
    );
  }

  return (
    <div className="flex flex-col">
      {allViews.map((item, i) => {
        return <div key={i}>{item}</div>;
      })}
    </div>
  );

  // const task_name: string = props.taskName ? props.taskName : "?";
  // // so yeah this code is messy - we should really massage things into a dict of run_id -> specific type...
  // // else we're assuming that the order of run ids and results matches.
  // const {
  //   observability_value,
  //   observability_type,
  //   observability_schema_version,
  // } = resultSummariesFiltered[0] as ResultsSummary;
  // if (
  //   observability_type === "pandas_describe" &&
  //   observability_schema_version === "0.0.1"
  // ) {
  //   return (
  //     <PandasDescribeV001View
  //       // results={props.resultsSummaries.map(item => item as PandasDescribeV0_0_1)}
  //       projectId={props.projectId}
  //       runIds={props.runs.map((run) => run.id as number)}
  //       results={resultSummariesFiltered.map(
  //         (item) => item.observability_value as PandasDescribeV0_0_1
  //       )}
  //     />
  //   );
  // } else if (
  //   observability_type === "primitive" &&
  //   observability_schema_version === "0.0.1"
  // ) {
  //   type PrimitiveType = {
  //     type: string;
  //     value: string;
  //   };
  //   // for each observability_value in resultSummariesFiltered, cast it to PrimitiveType
  //   // and put it into values
  //   const values = resultSummariesFiltered.map((item) => {
  //     return { runId: item.runId, ...item.observability_value };
  //   }) as ({ runId: number } & PrimitiveType)[];
  //   return (
  //     <div className="m-8">
  //       <GenericTable
  //         data={values.map((item) => {
  //           return [item.runId?.toString() || "", item];
  //         })}
  //         columns={[
  //           {
  //             displayName: "type",
  //             Render: (item: PrimitiveType) => {
  //               return (
  //                 <div className="flex flex-col">
  //                   <code className="text-sm">
  //                     {parsePythonType({ typeName: item.type })}
  //                   </code>
  //                 </div>
  //               );
  //             },
  //           },
  //           {
  //             displayName: "value",
  //             Render: (item: PrimitiveType) => {
  //               const [expanded, setExpanded] = useState(false);
  //               return (
  //                 <div className="flex flex-col w-144">
  //                   <pre
  //                     onClick={(e) => {
  //                       setExpanded(!expanded);
  //                       e.stopPropagation();
  //                     }}
  //                     className={`${
  //                       expanded ? "break-word whitespace-pre-wrap" : "truncate"
  //                     }  text-gray-500 cursor-cell`}
  //                   >
  //                     {item.value.toString()}
  //                   </pre>
  //                   {/* <code className="text-sm whitespace-pre-wrap">
  //                     {item.value.toString()}
  //                   </code> */}
  //                 </div>
  //               );
  //             },
  //           },
  //         ]}
  //         dataTypeName={"Run"}
  //         dataTypeDisplay={(item: string) => {
  //           return (
  //             <RunLink
  //               projectId={props.projectId}
  //               runId={parseInt(item) as number}
  //               setHighlightedRun={() => void 0}
  //               highlightedRun={null}
  //             ></RunLink>
  //           );
  //         }}
  //       />
  //     </div>
  //   );
  // } else if (
  //   observability_type === "dict" &&
  //   observability_schema_version === "0.0.2"
  // ) {
  //   return (
  //     <DictView
  //       runIds={props.runs.map((run) => run.id as number)}
  //       results={resultSummariesFiltered.map(
  //         (item) => (item.observability_value as any).value as object
  //       )}
  //     ></DictView>
  //   );
  // } else if (
  //   observability_type === "dagworks_describe" &&
  //   observability_schema_version === "0.0.3"
  // ) {
  //   return (
  //     <DAGWorksDescribeV003View
  //       projectId={props.projectId}
  //       runIds={props.runs.map((run) => run.id as number)}
  //       results={resultSummariesFiltered.map(
  //         (item) => item.observability_value as DAGWorksDescribeV0_0_3
  //       )}
  //     />
  //   );
  // }
  // type UnknownType = {
  //   unsupported_type: string;
  //   action: string;
  // };
  // const values = resultSummariesFiltered.map((item) => {
  //   return {
  //     value: item.observability_value as UnknownType,
  //     runId: item.runId,
  //   };
  // });
  // // TODO: this does not show all the outputs -- just the first one. This code is messy so not going to fix it now.
  // return (
  //   <div className="flex flex-col m-20">
  //     <span className="text-lg text-gray-800">
  //       We currently do not capture data summaries for run(s){" "}
  //       <code>[{runIds.join(", ")}]</code>
  //       for <code>{task_name}</code>. We are working on adding support for
  //       everything -- reach out if you need it and we can prioritize!
  //     </span>
  //     <div className="m-8">
  //       <GenericTable
  //         data={values.map((item) => {
  //           return [item.runId.toString() || "", item.value];
  //         })}
  //         columns={[
  //           {
  //             displayName: "type",
  //             Render: (item: UnknownType) => {
  //               return (
  //                 <div className="flex flex-col">
  //                   <code className="text-sm">{item.unsupported_type}</code>
  //                 </div>
  //               );
  //             },
  //           },
  //           {
  //             displayName: "action",
  //             Render: (item: UnknownType) => {
  //               return (
  //                 <div className="flex flex-col">
  //                   <span className="text-sm">{item.action}</span>
  //                 </div>
  //               );
  //             },
  //           },
  //         ]}
  //         dataTypeName={""}
  //       />
  //     </div>
  //   </div>
  // );
};
