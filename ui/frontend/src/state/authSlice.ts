import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "./store";
import { useAuthInfo } from "@propelauth/react";

export type AuthData = ReturnType<typeof useAuthInfo>;

export type AuthState = {
  authData: AuthData | null;
  localUserName: string | null;
  localAPIKey: string | null;
};

const initialState = {
  authData: null,
  localUserName: null,
  localAPIKey: null,
} as AuthState;

/**
 * Store basic auth information from propelauth
 */
export const authSlice = createSlice({
  initialState,
  name: "auth",
  reducers: {
    setAuth: (state: AuthState, action: PayloadAction<AuthData>) => {
      state.authData = action.payload;
    },
    logout: (state: AuthState) => {
      state.authData = null;
    },
    setLocalUserName: (state: AuthState, action: PayloadAction<string>) => {
      state.localUserName = action.payload;
    },
    setLocalAPIKey: (state: AuthState, action: PayloadAction<string>) => {
      state.localAPIKey = action.payload;
    }
  },
});

export default authSlice.reducer;

export const { logout, setAuth, setLocalUserName, setLocalAPIKey } = authSlice.actions;
export const useAuthData = (state: RootState) => state.auth.authData;
