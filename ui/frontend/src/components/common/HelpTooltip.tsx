import { AiFillQuestionCircle } from "react-icons/ai";

export const HelpTooltip: React.FC<{ description: string }> = (props) => {
  return (
    <div className="group flex relative">
      <span className="text-gray-400 hover:text-gray-500">
        <AiFillQuestionCircle className="text-lg" />
      </span>
      <span
        className="group-hover:opacity-100 w-48 z-50 transition-opacity bg-dwdarkblue p-1.5 text-sm text-gray-100 rounded-md absolute 
       opacity-0 m-4"
      >
        {props.description}
      </span>
    </div>
  );
};

export const LabelWithHelpTooltip: React.FC<{
  label: string;
  description: string;
}> = (props) => {
  return (
    <div className="col-span-3 sm:col-span-2 flex flex-row gap-2">
      <label className="block text-sm font-medium text-gray-700">
        {props.label}
      </label>
      <HelpTooltip description={props.description} />
    </div>
  );
};
