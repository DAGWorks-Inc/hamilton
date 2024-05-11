import JsonView from "@uiw/react-json-view";
import {
  AttributeDict1,
  AttributeDict2,
} from "../../../../../state/api/friendlyApi";
import { GenericTable } from "../../../../common/GenericTable";

export const Dict1View = (props: {
  taskName: string;
  runIds: number[];
  values: AttributeDict1[];
}) => {
  return (
    <GenericTable
      data={props.runIds.map((runId, i) => {
        return [runId.toString(), props.values[i]];
      })}
      columns={[
        {
          displayName: "data",
          Render: (value) => {
            return <JsonView value={value} collapsed={2} />;
          },
        },
      ]}
      dataTypeName={"run"}
    ></GenericTable>
  );
};

export const Dict2View = (props: {
  taskName: string;
  runIds: number[];
  values: AttributeDict2[];
}) => {
  return (
    <GenericTable
      data={props.runIds.map((runId, i) => {
        return [runId.toString(), props.values[i].value];
      })}
      columns={[
        {
          displayName: "data",
          Render: (value) => {
            return <JsonView value={value} collapsed={2} />;
          },
        },
      ]}
      dataTypeName={"run"}
    ></GenericTable>
  );
};
