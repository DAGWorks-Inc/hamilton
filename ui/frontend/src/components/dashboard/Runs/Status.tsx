// import { RunLogOut } from "../../../../state/api/backendApiRaw";
import { RunStatusType } from "../../../state/api/friendlyApi";

// export type RunStatus = RunLogOut["status"] | "NOT_RUN";

export const getColorFromStatus = (status: RunStatusType) => {
  switch (status) {
    case "SUCCESS":
      return {
        background: "bg-green-500",
        text: "text-green-500",
        html: "rgb(34,197,94)",
      };
    case "FAILURE":
      return {
        background: "bg-dwred",
        text: "text-dwred",
        html: "rgb(234,85,86)",
      };
    case "RUNNING":
      return {
        background: "bg-blue-500",
        text: "text-blue-500",
        html: "rgb(147,197,253);",
      };
    case "NOT_RUN":
      return {
        background: "bg-gray-500",
        text: "text-white",
        html: "rgb(209,213,219)",
      };
    default:
      return {
        background: "bg-gray-500",
        text: "text-white",
        html: "rgb(209,213,219)",
      };
  }
};
// TODO -- get this to use a shared component with any of the "pill" type displays
export const RunStatus = ({ status }: { status: RunStatusType }) => {
  return (
    <div
      className={`rounded-md text-white px-1 py-1 w-min ${
        getColorFromStatus(status).background
      }`}
    >
      {status.toLowerCase()}
    </div>
  );
};
