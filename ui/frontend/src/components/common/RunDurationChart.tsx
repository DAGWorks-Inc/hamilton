import {
  Chart as ChartJS,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Filler,
  Legend,
  CategoryScale,
} from "chart.js";
import { Line } from "react-chartjs-2";
import { RunStatusType } from "../../state/api/friendlyApi";
import { getColorFromStatus } from "../dashboard/Runs/Status";
// import { RunStatus, getColorFromStatus } from "../dashboard/Runs/Run/Status";

ChartJS.register(
  CategoryScale,
  BarElement,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Filler,
  Legend
);

export const RunDurationChart = (props: {
  runInfo: {
    name: string;
    runId: number;
    duration: number | undefined;
    state: RunStatusType;
  }[];
  w?: string;
  h?: string;
}) => {
  const data = {
    labels: props.runInfo.map((i) => `run: ${i.runId}`),
    datasets: [
      {
        categoryPercentage: 1.0,
        barPercentage: 0.99,
        label:
          "How long " +
          props.runInfo[0].name +
          " execution took for each run logged.",
        data: props.runInfo.map((i) => i.duration),
        fill: false,
        segment: {
          //eslint-disable-next-line @typescript-eslint/no-explicit-any
          borderColor: (ctx: any) => {
            const p2Index = ctx.p0.$context.dataIndex;
            const status = props.runInfo[p2Index].state;
            return getColorFromStatus(status).html;
          },
        },
        backgroundColor: props.runInfo.map(
          ({ state }) => getColorFromStatus(state).html
        ),
      },
    ],
  };
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    layout: {
      // autoPadding: true,
    },
    scales: {
      x: {
        display: true,
      },
      y: {
        display: true,
        ticks: {
          //eslint-disable-next-line @typescript-eslint/no-explicit-any
          callback: function (value: any) {
            return value + "s";
          },
        },
      },
    },
    plugins: {
      legend: {
        display: true,
        position: "top" as const,
      },
      title: {
        display: false,
        text: "Chart.js Line Chart",
      },
    },
  };
  const outerHeight = props.h || "h-[25px]";
  const outerWidth = props.w || "w-[250px]";
  const height = props.h ? props.h : "25px";
  const width = props.w ? "w-full" : "250px";

  return (
    <div className={`${outerHeight} ${outerWidth}`}>
      <Line
        data={data}
        options={options}
        height={height}
        width={width}
        className="items-center justify-center py-10"

        // height="25px"
        // width="150px"

        //   width={`${dimensions.width}px`}
      />
    </div>
  );
};
