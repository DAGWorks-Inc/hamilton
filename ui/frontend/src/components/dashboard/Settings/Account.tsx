import { Loading } from "../../common/Loading";
import { useUserInformation } from "../../../state/api/friendlyApi";

const Account = () => {
  const whoami = useUserInformation();
  if (whoami.isLoading || whoami.isFetching) {
    return <Loading />;
  }
  if (whoami.isError) {
    return <div>error</div>;
  }
  const user = whoami.data?.user.email || "";
  const orgs = whoami.data?.teams || [];
  return (
    <div>
      <div className="pl-9 text-gray-600">{user}</div>
      {orgs.map((org) => (
        <div key={org.name} className="pl-9 text-gray-600">
          {org.name}
        </div>
      ))}
    </div>
  );
};

export default Account;
