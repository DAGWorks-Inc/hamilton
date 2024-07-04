import { AttributeSchema1 } from "../../../../../state/api/friendlyApi";
import { GenericTable } from "../../../../common/GenericTable";
import {ColSchema} from "../../../../../state/api/backendApiRaw";


// export const Schema1View = (props: {
//     taskName: string;
//     runIds: number[];
//     values: AttributeSchema1[];
//   }) => {
//     const columns = [
//         {
//             displayName: "ColName",
//             Render: (item: AttributeSchema1) => <span>{item.name}</span>,
//         },
//         {
//             displayName: "Name",
//             Render: (item: AttributeSchema1) => <span>{item.name}</span>,
//         },
//         {
//             displayName: "Value",
//             Render: (item: AttributeSchema1) => <span>{item.value}</span>,
//         },
//   ];
//     const data = props.values.map(value => [value.id, value.name, value.value]);
//     return (
// <div>hi</div>
//     );
//   };

interface Schema1ViewProps {
  attributeSchemas: AttributeSchema1[];
  taskName: string;
  runIds: number[];
}
// export type RunIdToSchema = {
//   [key: number]: AttributeSchema1;
// };

// const buildRunIdToAttributeSchemaMapping = (props: Schema1ViewProps): { [key: number]: AttributeSchema1 } => {
//   const mapping: RunIdToSchema = {};
//
//   props.runIds.forEach((runId, index) => {
//     mapping[runId] = props.attributeSchemas[index];
//   });
//
//   return mapping;
// };

export const Schema1View: React.FC<Schema1ViewProps> = ({ attributeSchemas, taskName, runIds }) => {
    // Convert the AttributeSchema1 object into an array of its values
    // const runIdtoAttributeSchema = buildRunIdToAttributeSchemaMapping({ attributeSchemas, taskName, runIds });
    const schemaArray = Object.values(attributeSchemas[0]);
    console.log(attributeSchemas);
    console.log(runIds);

    // return (
    //     <GenericTable data={} columns={} dataTypeName={}></GenericTable>
    // )
    return (
        <table>
            <thead>
            <tr>
                <th>Column Name</th>
                <th>Type</th>
                <th>Nullable</th>
                <th>Metadata</th>
            </tr>
            </thead>
            <tbody>
            {schemaArray.map((col, index) => (
                <tr key={index}>
                    <td>{col.name}</td>
                    <td>{col.type}</td>
                    <td>{col.nullable}</td>
                    <td>{JSON.stringify(col.metadata)}</td>
                </tr>
            ))}
            </tbody>
        </table>
    );
}
