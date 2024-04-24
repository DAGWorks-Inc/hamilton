import { ReactNode } from "react";

export const ToolTip: React.FC<{ children: ReactNode; tooltip?: string }> = (
  props
) => {
  return (
    <div className="group relative inline-block z-50">
      {props.children}
      {/* This is a hack -- the overflow is hidden for the parent container, so we need to
         position it on the right of the icon.
         We'll need to redo it, just don't have the time now and this works... */}
      <span
        className="invisible group-hover:visible opacity-0 group-hover:opacity-100 transition
             bg-dwred text-white p-1 rounded absolute top-full -mt-5 mx-0 whitespace-nowrap z-50"
      >
        {props.tooltip}
      </span>
    </div>
  );
};
