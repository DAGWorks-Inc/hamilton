import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "./store";

/**
 * Basic project state -- we store a
 * project that has enough information to
 * query for git code/whatnot
 * TBD
 */
interface ProjectState {
  currentProjectID: number | undefined;
  hasSelectedProject: boolean;
}

const initialState: ProjectState = {
  currentProjectID: undefined, // Nothing to start
  hasSelectedProject: false,
};

export const projectSlice = createSlice({
  initialState,
  name: "project",
  reducers: {
    setProjectID: (state: ProjectState, action: PayloadAction<number>) => {
      state.currentProjectID = action.payload;
      state.hasSelectedProject = true;
    },
    unsetProjectID: (state: ProjectState) => {
      state.currentProjectID = undefined;
      state.hasSelectedProject = false;
    },
  },
});

export default projectSlice.reducer;

export const { setProjectID } = projectSlice.actions;

export const selectProjectID = (state: RootState) =>
  state.project.currentProjectID;

export const selectUserHasChosenProject = (state: RootState): boolean =>
  state.project.hasSelectedProject;
