import { AiFillCode } from "react-icons/ai";
import { BiAlarmExclamation, BiRun } from "react-icons/bi";
import { GoBook } from "react-icons/go";
import { IoGitNetworkSharp } from "react-icons/io5";
import { VscVersions } from "react-icons/vsc";
import { DAGTemplateWithData } from "../../state/api/friendlyApi";
export const navKeys = ["Structure", "Runs"];

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
  Structure: "STRUCTURE",
  Runs: "RUNS",
};

export const navigation = [
  {
    name: "Code",
    href: pathNav("code"),
    icon: AiFillCode,
    under: "Structure",
    requires: ["project", "projectVersion"],
  },
  {
    name: "Visualize",
    href: pathNav("visualize"),
    icon: IoGitNetworkSharp,
    under: "Structure",
    requires: ["project", "projectVersion"],
  },
  {
    name: "History",
    href: pathNav("runs"),
    icon: BiRun,
    under: "Runs",
    requires: ["project"],
  },
  {
    name: "Alerts",
    href: pathNav("alerts"),
    icon: BiAlarmExclamation,
    under: "Runs",
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
    name: "Versions",
    href: pathNav("versions"),
    icon: VscVersions,
    under: null,
    requires: ["project"],
  },
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
