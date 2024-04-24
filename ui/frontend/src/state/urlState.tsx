import { useParams } from "react-router-dom";
import { parseListOfInts } from "../utils/parsing";

/**
 * Gets the URL parameters -- note that these will be undefined if they're not
 * present in the URL. This is supported if we have not navigated to a page that
 * has URL-specific information (runs, tasks, versions, projects, etc...).
 */
export const useURLParams = () => {
  const {
    projectId: projectIdsRaw,
    versionId: versionIdsRaw,
    runId: runIdRaw,
    taskName: taskNameRaw,
  } = useParams();
  return {
    projectId: projectIdsRaw ? parseInt(projectIdsRaw) : undefined,
    versionIds: versionIdsRaw ? parseListOfInts(versionIdsRaw) : undefined,
    runIds: runIdRaw ? parseListOfInts(runIdRaw) : undefined,
    taskName: taskNameRaw ? taskNameRaw : undefined,
  };
};
