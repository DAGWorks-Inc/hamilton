import React from "react";

export const Loading = () => {
  return (
    <div className="flex items-center justify-center space-x-2 w-full h-full">
      <div className="w-8 h-8 bg-dwred/50 rounded-full animate-pulse animation-delay-0"></div>
      <div className="w-8 h-8 bg-dwdarkblue/50 rounded-full animate-pulse animation-delay-500"></div>
      <div className="w-8 h-8 bg-dwlightblue/50 rounded-full animate-pulse animation-delay-1000"></div>
    </div>
  );
};
