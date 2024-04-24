import ReactDOM from "react-dom/client";
import "./index.css";
import { App } from "./App";
import reportWebVitals from "./reportWebVitals";
import { persistor, store } from "./state/store";
// import {store} from "./state/store"
import { Provider } from "react-redux";
// This is a weird bug: https://github.com/rt2zz/redux-persist/issues/1166
// eslint-disable-next-line @typescript-eslint/no-unused-vars
import { PersistGate } from "redux-persist/integration/react";
import { RequiredAuthProvider, RedirectToLogin } from "@propelauth/react";
import { Loading } from "./components/common/Loading";
import { PostHogProvider } from "posthog-js/react";
import TimeAgo from "javascript-time-ago";
import en from "javascript-time-ago/locale/en.json";
import { LocalLoginProvider } from "./ee/auth/Login";

const posthogOptions = {
  api_host: process.env.REACT_APP_PUBLIC_POSTHOG_HOST,
};

const root = ReactDOM.createRoot(
  document.getElementById("root") as HTMLElement
);

const NoopTelemetryProvider = ({ children }: { children: React.ReactNode }) => {
  return <>{children}</>;
};

TimeAgo.addDefaultLocale(en);

const AuthProvider =
  process.env.REACT_APP_AUTH_MODE === "local"
    ? LocalLoginProvider
    : RequiredAuthProvider;

const TelemetryProvider =
  process.env.REACT_APP_USE_POSTHOG === "true"
    ? PostHogProvider
    : NoopTelemetryProvider;

root.render(
  // <React.StrictMode>
  <AuthProvider
    authUrl={process.env.REACT_APP_AUTH_URL as string}
    displayWhileLoading={<Loading />}
    displayIfLoggedOut={<RedirectToLogin />}
  >
    <Provider store={store}>
      <PersistGate loading={<Loading />} persistor={persistor}>
        <TelemetryProvider
          apiKey={process.env.REACT_APP_PUBLIC_POSTHOG_KEY}
          options={posthogOptions}
        >
          <App />
        </TelemetryProvider>
      </PersistGate>
    </Provider>
  </AuthProvider>
  // </React.StrictMode>
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
