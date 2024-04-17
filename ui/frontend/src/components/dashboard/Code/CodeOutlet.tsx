import { Loading } from "../../common/Loading";
import { useData as useDashboardData } from "../Dashboard";
import { Code } from "./Code";

export const CodeOutlet = () => {
  const { dagTemplates, project } = useDashboardData();

  if (dagTemplates === undefined) {
    return <Loading />;
  }
  return <Code project={project} dagTemplates={dagTemplates} />;
};
