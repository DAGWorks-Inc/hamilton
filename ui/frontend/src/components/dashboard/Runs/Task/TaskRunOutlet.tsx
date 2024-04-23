import { useNavigate } from "react-router-dom";
import { useRunData } from "../Run/Run";
import { TaskView } from "./Task";
import { MAX_RUNS_QUERIED } from "../Runs";
import {
  useIndividualNodeRunData,
  useLatestDAGRuns,
} from "../../../../state/api/friendlyApi";
import { useURLParams } from "../../../../state/urlState";
import { Loading } from "../../../common/Loading";
import { ErrorPage } from "../../../common/Error";

export const TaskRunOutlet = () => {
  const { runs, dagTemplates: projectVersions } = useRunData();
  const { taskName, projectId } = useURLParams();
  const availableRuns = useLatestDAGRuns({
    projectId: projectId as number,
    limit: MAX_RUNS_QUERIED,
  });

  const nav = useNavigate();

  const nodeRunData = useIndividualNodeRunData({
    dagRunIds: runs.map((i) => i.id as number).join(","),
    nodeName: taskName as string,
  });

  if (
    availableRuns.isLoading ||
    availableRuns.isFetching ||
    availableRuns.isUninitialized ||
    nodeRunData.isLoading ||
    nodeRunData.isFetching ||
    nodeRunData.isUninitialized
  ) {
    return <Loading />;
  }
  if (availableRuns.isError) {
    return <ErrorPage message={`Failed to load runs`} />;
  }
  if (nodeRunData.isError) {
    return <ErrorPage message={`Failed to load node run data`} />;
  }
  /**
   * Sets the URL to replace /runs/<comma-separated list of run ids>/task/<task name> with the new list of run ids
   *
   * @param runIds
   */
  const setRuns = (runIds: number[]) => {
    nav(
      `/dashboard/project/${projectId}/runs/${runIds.join(
        ","
      )}/task/${taskName}`
    );
  };
  return (
    <TaskView
      taskName={taskName as string}
      projectId={projectId as number}
      projectVersions={projectVersions}
      // TODO -- we should be able to filter this out when we have more info
      // But for now it might just be too much
      knownRunIds={availableRuns.data?.map((i) => i.id as number) || []}
      nodeRunData={nodeRunData.data || []}
      setRuns={setRuns}
      fullRuns={runs} // we don't really need the data -- we should just compute the node links here...
    />
  );
};
