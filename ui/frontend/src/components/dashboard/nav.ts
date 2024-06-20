import { AiFillCode } from "react-icons/ai";
import { BiRun } from "react-icons/bi";
import { GoBook } from "react-icons/go";
import { IoGitNetworkSharp } from "react-icons/io5";
import { DAGTemplateWithData } from "../../state/api/friendlyApi";
export const navKeys = [];

const pathNav = (pathEnd: string) => {
  return (
    project: string | undefined,
    projectVersion: string | undefined,
    requires: string[]
  ) => {
    if (projectVersion === undefined || !requires.includes("projectVersion")) {
      return `/dashboard/project/${project}/${pathEnd}`;
    }
    return `/dashboard/project/${project}/version/${projectVersion}/${pathEnd}`;
  };
};

// Mapping of nav to help in the HelpVideo file
export const NAV_HELP = {
  Versions: "VERSIONS",
  Visualize: "STRUCTURE",
  History: "RUNS",
};

export const navigation = [
  {
    name: "Visualize",
    href: pathNav("visualize"),
    icon: IoGitNetworkSharp,
    under: null,
    requires: ["project", "projectVersion"],
  },
  {
    name: "Code",
    href: pathNav("code"),
    icon: AiFillCode,
    under: null,
    requires: ["project", "projectVersion"],
  },
  {
    name: "History",
    href: pathNav("runs"),
    icon: BiRun,
    under: null,
    requires: ["project"],
  },
  // {
  //   name: "Materialize",
  //   href: pathNav("materialize"),
  //   icon: FiDatabase,
  //   under: null,
  //   requires: ["project", "projectVersion"],
  // },
  // {
  //   name: "Report",
  //   href: pathNav("report"),
  //   icon: TbReportAnalytics,
  //   under: null,
  //   requires: ["project", "projectVersion"],
  // },
  {
    name: "Catalog",
    href: pathNav("catalog"),
    icon: GoBook,
    under: null,
    requires: ["project"],
  },
];

export const resolveNav = (
  project: string | undefined,
  projectVersion: DAGTemplateWithData[] | undefined
) => {
  const available: string[] = [];
  if (project !== undefined) {
    available.push("project");
  }
  if (projectVersion !== undefined) {
    available.push("projectVersion");
  }
  return navigation
    .filter((navItem) =>
      navItem.requires.map((item) => available.includes(item)).every((i) => i)
    )

    .map((navItem) => {
      return {
        ...navItem,
        href: navItem.href(
          project,
          projectVersion?.map((i) => i.id).join(","),
          navItem.requires
        ),
      };
    });
};
