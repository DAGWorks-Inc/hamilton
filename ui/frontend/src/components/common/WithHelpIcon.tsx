import { ReactNode, useContext } from "react";
import { HelpVideos, TutorialContext } from "../tutorial/HelpVideo";
import { FaQuestionCircle } from "react-icons/fa";

export const WithHelpIcon = (props: {
  children: ReactNode;
  whichIcon: keyof typeof HelpVideos;
  translate?: string;
}) => {
  const { setLoomVideoOpen, includeHelp, setCurrentLoomVideo } =
    useContext(TutorialContext);
  return (
    <div>
      {includeHelp ? (
        <FaQuestionCircle
          className={`text-yellow-400 text-sm fixed hover:cursor-pointer hover:scale-150 ${
            props.translate || ""
          }`}
          onClick={() => {
            setCurrentLoomVideo(props.whichIcon);
            setLoomVideoOpen(true);
          }}
        />
      ) : null}
      {props.children}
    </div>
  );
};
