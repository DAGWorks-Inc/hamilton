import {
  AttributeDict1,
  AttributeDict2,
} from "../../../../../state/api/friendlyApi";
import { GenericTable } from "../../../../common/GenericTable";
import { JSONTree } from "react-json-tree";

const theme = {
  scheme: "shapeshifter",
  author: "tyler benziger (http://tybenz.com)",
  base00: "#000000",
  base01: "#040404",
  base02: "#102015",
  base03: "#343434",
  base04: "#555555",
  base05: "#ababab",
  base06: "#e0e0e0",
  base07: "#f9f9f9",
  base08: "#e92f2f",
  base09: "#e09448",
  base0A: "#dddd13",
  base0B: "#0ed839",
  base0C: "#23edda",
  base0D: "#3b48e3",
  base0E: "#f996e2",
  base0F: "#69542d",
};

export const Dict1View = (props: {
  taskName: string;
  runIds: number[];
  values: AttributeDict1[];
}) => {
  return (
    <GenericTable
      data={props.runIds.map((runId, i) => {
        console.log(props.values[i]);
        return [runId.toString(), props.values[i]];
      })}
      columns={[
        {
          displayName: "data",
          Render: (value) => {
            return (
              <JSONTree
                invertTheme={true}
                data={value}
                theme={theme}
                hideRoot
                shouldExpandNodeInitially={() => false}
              />
            );
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
            return (
              <JSONTree
                invertTheme={true}
                data={value}
                theme={theme}
                hideRoot
                shouldExpandNodeInitially={() => false}
              />
            );
          },
        },
      ]}
      dataTypeName={"run"}
    ></GenericTable>
  );
};
