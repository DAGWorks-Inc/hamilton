import { ErrorPage } from "../../common/Error";
import { Loading } from "../../common/Loading";
import { CatalogView } from "./SearchTable";
import { useURLParams } from "../../../state/urlState";
import { Project, useProjectByID } from "../../../state/api/friendlyApi";

// const DEFAULT_NUMBER_OF_VERSIONS = 5;
// const DEFAULT_NUMBER_OF_RUNS_PER_VERSION = 3;
export const CatalogOutlet = () => {
  const { projectId } = useURLParams();
  const project = useProjectByID({ projectId: projectId as number });

  if (project.isError) {
    return (
      <ErrorPage message={`Failed to load project with ID: ${projectId}`} />
    );
  } else if (
    project.isLoading ||
    project.isFetching ||
    project.isUninitialized
  ) {
    return <Loading />;
  }
  return <CatalogView project={project.data as Project} />;
};
