import { useContext } from "react";
import { UserContext } from "./Login";
import { ProjectLogInstructions } from "../../components/dashboard/Project/ProjectLogInstructions";

export const LocalAccount = () => {
  const localAccount = useContext(UserContext)?.username;
  return (
      <div className="mt-20">
        <h1 className="text-3xl font-bold text-gray-700 pl-5 pb-4">Email: {localAccount}</h1>
        <ProjectLogInstructions
          projectId={undefined}
          username={localAccount || ""}
          canWrite={true}
          hideAPIKey
        ></ProjectLogInstructions>
      </div>
  );
};
