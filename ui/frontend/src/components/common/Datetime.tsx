// DateTimeDisplay.tsx
import React, { FC } from "react";
import dayjs from "dayjs";
import { durationFormat } from "../../utils";

interface DateTimeDisplayProps {
  datetime: string;
}

export const DateTimeDisplay: FC<DateTimeDisplayProps> = ({ datetime }) => {
  // Format the datetime prop using dayjs
  const formattedDate = dayjs(datetime).format("MMMM D, YYYY");
  const formattedTime = dayjs(datetime).format("h:mma");

  return (
    <div className="flex flex-row gap-2">
      <div className="bg-dwdarkblue/40 p-2 rounded-md shadow w-min">
        <span className="text-sm text-white">{formattedDate}</span>
      </div>
      <div className="bg-dwdarkblue/40 p-2 rounded-md shadow w-min hidden md:block">
        <span className="text-sm text-white">{formattedTime}</span>
      </div>
    </div>
  );
};

export const DurationDisplay = (props: {
  startTime: string | undefined;
  endTime: string | undefined;
  currentTime: Date;
}) => {
  const {
    formattedHours,
    formattedMinutes,
    formattedSeconds,
    formattedMilliseconds,
  } = durationFormat(
    props.startTime || undefined,
    props.endTime || undefined,
    new Date()
  );
  // const out = `${formattedHours}:${formattedMinutes}:${formattedSeconds}.${formattedMilliseconds}`;
  const getTextColor = (highlighted: boolean) => {
    if (!highlighted) {
      return "text-gray-300";
    }
    return ""; // default to standrd
  };
  const highlightHours = formattedHours !== "00";
  const highlightMinutes = formattedMinutes !== "00" || highlightHours;
  const highlightSeconds = true;
  const highlightMilliseconds = true;
  return (
    <div className="font-semibold flex gap-0">
      <span className={getTextColor(highlightHours)}>{formattedHours}</span>
      <span className={getTextColor(highlightHours && highlightMinutes)}>
        :
      </span>
      <span className={`${getTextColor(highlightMinutes)}`}>
        {formattedMinutes}
      </span>
      <span className={getTextColor(highlightMinutes && highlightSeconds)}>
        :
      </span>
      <span className={`${getTextColor(highlightSeconds)}`}>
        {formattedSeconds}
      </span>
      <span className={getTextColor(highlightSeconds && highlightMilliseconds)}>
        .
      </span>
      <span
        className={`${
          highlightMilliseconds
            ? getTextColor(highlightMilliseconds)
            : "text-gray-200"
        }`}
      >
        {formattedMilliseconds}
      </span>
    </div>
  );
};
