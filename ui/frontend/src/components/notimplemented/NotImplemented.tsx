import { useLocation } from "react-router-dom";

interface LocationState {
  feature: string;
}
const notImplementedFeatures = new Map(
  Object.entries({
    signin_with_google: {
      title: "Signing in with google",
      state:
        "We are actively working on building out our authentication system.",
    },
    signup: {
      title: "Signing up",
      state:
        "We are actively working on building out our authentication system.",
    },
    signin_with_github: {
      title: "Signing in with github",
      state:
        "We are actively working on building out our authentication system.",
    },
  })
);

export const NotImplemented = () => {
  /**
   * Simple page for "Not implemented features"
   * We shouldn't have too many but its kind of nice to have a
   * placeholder for publishing, especially early on.
   * E.G. bells and whistles should probably make it to a
   * github ticket, whereas needed features will make it here
   * (and also to a github ticket)
   */
  const { feature } = useLocation().state as LocationState;
  const featureInfo = notImplementedFeatures.get(feature);
  if (featureInfo === undefined) {
    return <div>Feature {feature} has not yet been implemented.</div>;
  }
  return (
    <div>
      <h1>{featureInfo.title} has not yet been implemented.</h1>
      <p>{featureInfo.state}</p>
    </div>
  );
};
