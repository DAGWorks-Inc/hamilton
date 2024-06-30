import { createApi, fetchBaseQuery } from "@reduxjs/toolkit/query/react";
import { RootState } from "../store";

/**
 * Initializes an empty API slice
 * We do this so we can generate it later
 * Unfortunately, this is a total mess due to redux state.
 * What we have to do is await the state value to get to what we want, the token we have.
 * What we really should be doing is using a redux integration...
 *
 * Unfortunately the useEffect hook with dispatch won't invalidate the cache...
 * @param getState
 */
const awaitTokenAvailable = async (getState: () => RootState) => {
  const token = (getState() as RootState).auth.authData;
  if (token === null) {
    setTimeout(() => awaitTokenAvailable(getState), 10);
  }
};

const baseQuery = fetchBaseQuery({
  baseUrl: "/",
  timeout: 10000,
  prepareHeaders: async (headers, { getState }) => {
    const getStateTyped = getState as () => RootState;
    await awaitTokenAvailable(getStateTyped);
    const authData = getStateTyped().auth.authData;
    // @ts-ignore
    const token = authData?.accessToken;
    if (token) {
      headers.set("Authorization", `Bearer ${token}`);
    } else if (process.env.REACT_APP_AUTH_MODE === "local") {
      headers.set("x-api-user", getStateTyped().auth.localUserName || "");
      headers.set("x-api-key", getStateTyped().auth.localAPIKey || "");
    }
    return headers;
  },
});

export const emptySplitApi = createApi({
  baseQuery: baseQuery,
  endpoints: () => ({}),
  reducerPath: "backendApi",
});
