import { Link } from "react-router-dom";

export const Alerts = () => {
  const currentURL = new URL(window.location.href);
  const pathParts = currentURL.pathname.split("/");
  // Remove the last part of the path
  pathParts.pop();
  const modifiedPath = pathParts.join("/") + "/runs";
  return (
    <div className="p-16 text-gray-900 text-center">
      Sorry, this feature is not available to you yet. If you are interested in
      trying it out, please reach out to us - info@dagworks.io! In the
      meanwhile, check out your{" "}
      <Link className="text text-dwlightblue hover:underline" to={modifiedPath}>
        run history{" "}
      </Link>{" "}
      to dive into your executions.
    </div>
  );
};
