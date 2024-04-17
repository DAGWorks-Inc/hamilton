export const Legend = () => {
  const legendItems = [
    { text: "Current", bgClass: "bg-green-500 text-white" },
    { text: "Upstream", bgClass: "bg-dwred text-white" },
    { text: "Downstream", bgClass: "bg-dwlightblue text-white" },
    { text: "Default", bgClass: "bg-dwdarkblue text-white" },
    { text: "Unrelated", bgClass: "bg-gray-400 text-white" },
    {
      text: "Input",
      bgClass: "bg-white text-dwdarkblue border-2 border-dwdarkblue",
    },
  ];

  return (
    <div className="flex flex-row gap-2">
      {legendItems.map((item, index) => (
        <div
          key={index}
          className={`px-3 py-1 shadow-sm flex flex-row items-center align-middle justify-center rounded-lg w-max text-sm ${item.bgClass}`}
        >
          {item.text}
        </div>
      ))}
    </div>
  );
};
