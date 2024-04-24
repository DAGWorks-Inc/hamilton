export const DashboardItem = (props: {
  children: React.ReactNode;
  extraWide?: boolean;
}) => {
  return (
    <div className="flex flex-col bg-white rounded-md shadow-md m-3 max-w-full overflow-x-auto flex-grow">
      {props.children}
    </div>
  );
};
export const MetricDisplay = (props: {
  number: number;
  label: string | JSX.Element;
  textColorClass: string;
  isSubset: boolean;
  numberFormatter?: (num: number) => string;
}) => {
  const numberDisplay = props.numberFormatter
    ? props.numberFormatter(props.number)
    : props.number;
  return (
    <div className={`flex flex-col items-center p-3 max-w-xl`}>
      <div className={`text-5xl font-semibold ${props.textColorClass}`}>
        {`${numberDisplay}${props.isSubset ? "+" : ""}`}
      </div>
      <div className={`${props.textColorClass} font-light`}>{props.label}</div>
    </div>
  );
};
