import React from "react";
import { useData as useDashboardData } from "../Dashboard";
import Versions from "./Versions";

export const VersionsOutlet = () => {
  const { project } = useDashboardData();
  return <Versions project={project} />;
};
