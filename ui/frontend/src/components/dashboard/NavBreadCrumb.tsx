import { ChevronRightIcon, HomeIcon } from "@heroicons/react/20/solid";
import { DAGTemplateWithoutData, Project } from "../../state/api/friendlyApi";

export const NavBreadCrumb = (props: {
  project: Project;
  dagTemplates: DAGTemplateWithoutData[];
}) => {
  const getElements = () => {
    const pathNameRelativeToDashboard = location.pathname.split("/").slice(2);
    return [
      // ...elements,
      ...pathNameRelativeToDashboard.map((pathName, index) => {
        // This is a little hacky of a way to get the actual names displayed
        // TODO -- think this through a little cleaner
        const linkName =
          index == 1
            ? props.project.name
            : index == 3
            ? props.dagTemplates[0]?.name
            : pathName;
        return (
          // TODO -- enable when we can get this working -- we just need to link for links that exist
          // Should be easy I just don't have time now
          <span
            // to={
            //   "/dashboard/" +
            //   pathNameRelativeToDashboard.slice(0, index + 1).join("/")
            // }
            key={linkName}
            className="ml-4 font-medium hover:text-gray-800"
          >
            {linkName}
          </span>
        );
      }),
    ];
  };

  // Relative to the dashboard so that we can view it

  const elements = getElements();

  return (
    <nav className="flex max-w-full bg-transparent" aria-label="Breadcrumb">
      <ol role="list" className="flex items-center space-x-4 flex-wrap">
        <li>
          <div>
            <a href="#" className=" hover:text-gray-800">
              <HomeIcon className="h-5 w-5 flex-shrink-0" aria-hidden="true" />
              <span className="sr-only">Home</span>
            </a>
          </div>
        </li>
        {elements.map((element, index) => (
          <li key={index}>
            <div className="flex items-center">
              <ChevronRightIcon
                className="h-5 w-5 flex-shrink-0 "
                aria-hidden="true"
              />
              {element}
            </div>
          </li>
        ))}
      </ol>
    </nav>
  );
};
