import React from "react";
import { Link } from "react-router-dom";

export const Home = () => {
  /**
   * Just some divs to represent home/navigation
   * TODO -- delete me. I'm unecessary past rudimentary development.
   */
  return (
    <div>
      <h1>Home</h1>
      <p>
        <Link to="/login/">Login</Link>
      </p>
      <p>
        <Link to="/signup">Sign up</Link>
      </p>
      <p>
        <Link to="/dashboard">Dashboard</Link>
      </p>
    </div>
  );
};
