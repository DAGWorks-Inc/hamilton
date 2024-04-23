import { Loading } from "../../common/Loading";
import { useUserInformation } from "../../../state/api/friendlyApi";
import { useRedirectFunctions } from "@propelauth/react";
import { ErrorPage } from "../../common/Error";

export const Account = () => {
  const userInfo = useUserInformation();
  const { redirectToOrgPage } = useRedirectFunctions();
  if (userInfo.isLoading || userInfo.isFetching) {
    return <Loading />;
  }
  if (userInfo.isError) {
    return (
      <ErrorPage
        message="Unable to retrieve account information from the backend,
      please reach out to the DAGWorks team for support. Please, however, try to refresh first."
      />
    );
  }
  const user = userInfo.data?.user.email || "";
  const orgs = userInfo.data?.teams || [];
  return (
    <div className="flex  flex-col gap-3 mt-10">
      <div className="border-b border-gray-200 bg-white px-4 py-5 sm:px-6">
        <h2 className="text-base font-semibold leading-6 text-gray-900">
          Account
        </h2>
      </div>
      <div className="flex flex-row pl-6 text-gray-600 gap-2">
        <span className="text-gray-900 font-semibold">Username: </span>
        <span className="font-normal">{user}</span>
      </div>
      <div className="flex flex-row pl-6 text-gray-600 gap-2">
        <span className="text-gray-900 font-semibold">Workspaces: </span>
        {orgs.length > 0 ? (
          orgs.map((org) => {
            return (
              <span
                onClick={() =>
                  redirectToOrgPage(org.auth_provider_organization_id)
                }
                key={org.name}
                className="font-normal  text-dwlightblue px-2 rounded-md hover:underline hover:cursor-pointer"
              >
                {org.name}
              </span>
            );
          })
        ) : (
          <span className="font-normal">
            Upgrade to the paid plan to collaborate with your team!
          </span>
        )}
      </div>
      <div>
        <div className="px-4 py-5 sm:px-6">
          <span className="font-normal  text-dwlightblue">
            <a href="https://www.dagworks.io/terms">Terms of Use</a>
          </span>
        </div>
      </div>
    </div>
  );
};

export default Account;
