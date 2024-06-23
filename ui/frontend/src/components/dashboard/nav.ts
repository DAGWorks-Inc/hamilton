import { AiFillCode } from "react-icons/ai";
import { IoGitNetworkSharp, IoExtensionPuzzle, IoFileTrayFull, IoGitMerge} from "react-icons/io5";
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
  Dataflow: "STRUCTURE",
  Runs: "RUNS",
};

export const navigation = [
  {
    name: "Dataflow",
    href: pathNav("visualize"),
    icon: IoGitNetworkSharp,
    under: null,
    requires: ["project", "projectVersion"],
  },
  {
    name: "Assets",
    href: pathNav("catalog"),
    icon: IoExtensionPuzzle,
    under: null,
    requires: ["project"],
  },
  {
    name: "Code",
    href: pathNav("code"),
    icon: AiFillCode,
    under: null,
    requires: ["project", "projectVersion"],
  },
  {
    name: "Runs",
    href: pathNav("runs"),
    icon: IoFileTrayFull,
    under: null,
    requires: ["project"],
  },
  {
    name: "Versions",
    href: pathNav("versions"),
    icon: IoGitMerge,
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
