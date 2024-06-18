import { AttributeHTML1 } from "../../../../../state/api/friendlyApi";
import { GenericTable } from "../../../../common/GenericTable";


interface HTMLDisplayProps {
  htmlContent: string;
}

const HTMLDisplay: React.FC<HTMLDisplayProps> = ({ htmlContent }) => {
  return (
    <div dangerouslySetInnerHTML={{ __html: htmlContent }} />
  );
};

export const HTML1View = (props: {
    taskName: string;
    runIds: number[];
    values: AttributeHTML1[];
  }) => {
    return (
      <GenericTable
        data={props.runIds.map((runId, i) => {
          return [runId.toString(), {props: props.values[i].html}];
        })}
        columns={[
          {
            displayName: "data",
            Render: (value) => {
              return <HTMLDisplay htmlContent={value.props}/>;
            },
          },
        ]}
        dataTypeName={"run"}
      ></GenericTable>
    );
  };
