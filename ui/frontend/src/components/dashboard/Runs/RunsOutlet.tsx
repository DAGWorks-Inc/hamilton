import { useData as useDashboardData } from "../Dashboard";
import Runs from "./Runs";

export const RunsOutlet = () => {
  const { project } = useDashboardData();
  return <Runs project={project} />;
};

export default RunsOutlet;
