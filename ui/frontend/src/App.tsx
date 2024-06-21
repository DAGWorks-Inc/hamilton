import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { useContext, useEffect, useState } from "react";
import { useAuthInfo } from "@propelauth/react";
import { useAppDispatch } from "./state/hooks";
import { setAuth, setLocalUserName } from "./state/authSlice";
// import Run from "./components/dashboard/Runs/Run/Run";
import Dashboard from "./components/dashboard/Dashboard";
import {
  TutorialContext,
  HelpModal,
  HelpVideos,
} from "./components/tutorial/HelpVideo";
import { usePostHog } from "posthog-js/react";
import { Alerts } from "./components/dashboard/Alerts";
import Account from "./components/dashboard/Account/Account";
import { ProjectsView } from "./components/dashboard/Project/Projects";
import { ProjectView } from "./components/dashboard/Project/Project";
import { VersionsOutlet } from "./components/dashboard/Versions/VersionsOutlet";
import RunsOutlet from "./components/dashboard/Runs/RunsOutlet";
import { CatalogOutlet } from "./components/dashboard/Catalog/CatalogOutlet";
import { Welcome } from "./components/dashboard/Welcome";
import Run from "./components/dashboard/Runs/Run/Run";
import { TaskRunOutlet } from "./components/dashboard/Runs/Task/TaskRunOutlet";
import { CodeOutlet } from "./components/dashboard/Code/CodeOutlet";
import { VisualizeOutlet } from "./components/dashboard/Visualize/VisualizeOutlet";
import Settings from "./components/dashboard/Settings/Settings";
import { UserContext } from "./auth/Login";
import { LocalAccount } from "./auth/LocalAccount";


export const localMode = process.env.REACT_APP_AUTH_MODE === "local";

const useAuthInfoBasedOnProcEnv = () => {
  if (localMode) {
    return null; // No auth info
  } else {
    // TODO -- push into component to ensure its static
    // eslint-disable-next-line
    return useAuthInfo();
  }
};

export const App = () => {
  const authInfo = useAuthInfoBasedOnProcEnv();
  const posthog = usePostHog();
  const dispatch = useAppDispatch();
  useEffect(() => {
    if (authInfo) {
      dispatch(setAuth(authInfo));
    }
  }, [authInfo, dispatch]);
  const [isTutorialVideoOpen, setTutorialVideoOpen] = useState(false);
  let userName =
    authInfo == null || authInfo.loading ? undefined : authInfo.user?.email;

  if (userName === undefined) {
    // This is a pretty quick hack for two oseparate authentication systems
    // This just bypasses it and uses the local one
    // TODO -- unify the local + propelauth system into a single so we can polymorphically switch between
    // eslint-disable-next-line
    userName = useContext(UserContext)?.username;
  }

  useEffect(() => {
    if (localMode && userName) {
      dispatch(setLocalUserName(userName));
    }
  }, [userName, dispatch]);
  const [currentLoomVideo, setCurrentLoomVideo] = useState<
    keyof typeof HelpVideos | undefined
  >(undefined);

  useEffect(() => {
    if (userName) {
      posthog?.identify(userName, {
        email: userName,
      });
    }
  }, [posthog, userName]);

  return (
    <div>
      {currentLoomVideo && (
        <HelpModal
          whichLoom={currentLoomVideo}
          open={isTutorialVideoOpen}
          setOpen={setTutorialVideoOpen}
        />
      )}
      <TutorialContext.Provider
        value={{
          currentLoomVideo: currentLoomVideo,
          setCurrentLoomVideo: setCurrentLoomVideo,
          open: isTutorialVideoOpen,
          setLoomVideoOpen: setTutorialVideoOpen,
          includeHelp: true,
        }}
      >
        <BrowserRouter>
          <Routes>
            {localMode && (<Route path="/" element={<Navigate to="/dashboard/account" />} />)}
            {!localMode && (<Route path="/" element={<Navigate to="/dashboard/welcome" />} />)}
            <Route path="/dashboard" element={<Dashboard />}>
              {!localMode && (<Route path="welcome" element={<Welcome />} />)}
              {!localMode && (<Route path="" element={<Navigate to="welcome" />} />)}
              {localMode && (<Route path="" element={<Navigate to="projects" />} />)}
              <Route path="projects" element={<ProjectsView />} />
              <Route
                path="project"
                element={<Navigate to="/dashboard/projects" />}
              />
              <Route
                path="project/:projectId/"
                // TODO -- pass the project in as a parameter so we don't need to grab global state in the project view
                element={<ProjectView />}
                // element={<div> TODO -- implement project </div>}
              ></Route>
              {
                <Route
                  path="project/:projectId/catalog"
                  element={<CatalogOutlet />}
                  // element={<div> TODO -- implement catalog </div>}
                />
              }
              <Route
                path="project/:projectId/versions"
                element={<VersionsOutlet />}
              />
              <Route
                path="project/:projectId/version/:versionId"
                element={<Navigate to="visualize" />}
              />
              <Route
                index
                path="project/:projectId/version/:versionId/code"
                element={<CodeOutlet />}
              />
              <Route
                path="project/:projectId/version/:versionId/visualize"
                element={<VisualizeOutlet />}
              />
              <Route
                path="project/:projectId/version"
                element={<Navigate to={"../"} />}
              />
              <Route path="project/:projectId/runs" element={<RunsOutlet />} />
              <Route path="project/:projectId/alerts" element={<Alerts />} />
              {/* Nested as it has to share state */}
              <Route path="project/:projectId/runs/:runId" element={<Run />}>
                <Route path="task/:taskName" element={<TaskRunOutlet />} />
              </Route>
              <Route path="/dashboard/settings" element={<Settings />} />
              {authInfo && (
                <Route path="/dashboard/account" element={<Account />} />
              )}
              {localMode && (
                <Route path="/dashboard/account" element={<LocalAccount />} />
              )}
            </Route>
            <Route path="*" element={<Navigate to="/dashboard" />} />
          </Routes>
        </BrowserRouter>
      </TutorialContext.Provider>
    </div>
  );
};
