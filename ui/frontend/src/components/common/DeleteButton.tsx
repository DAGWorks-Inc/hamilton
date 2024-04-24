import { XMarkIcon } from "@heroicons/react/20/solid";

export const DeleteButton = (props: {
  deleteMe: () => void;
  deleteType: string;
  canDelete: boolean;
}) => {
  if (!props.canDelete) return <></>;
  return (
    <button
      onClick={() => props.deleteMe()}
      type="button"
      className="inline-flex items-center gap-x-1.5 rounded-md
              bg-dwred py-2 px-3 text-sm font-semibold text-white shadow-sm
              hover:bg-dwred/80 focus-visible:outline focus-visible:outline-2
              focus-visible:outline-offset-2 focus-visible:outline-dwred"
    >
      <XMarkIcon className="-ml-0.5 h-5 w-5" aria-hidden="true" />
      {props.deleteType}
    </button>
  );
};
