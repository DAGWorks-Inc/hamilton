import React, { useState, createContext } from "react";
import { useLocalStorage } from "../utils/localStorage";

type UserContextType = {
  username: string;
  setUsername: (username: string) => void;
};

// Creating a context with a default value and type annotation
export const UserContext = createContext<UserContextType | undefined>(
  undefined
);

export const LocalLoginProvider = (props: { children: React.ReactNode }) => {
  const [localUsername, setLocalUsername] = useState<string>("");
  const isValidEmail = localUsername.includes("@"); // Quick hack -- todo: use email-validator
  const [username, setUsername] = useLocalStorage(
    "hamilton-username",
    undefined
  );
  if (username) {
    return (
      <UserContext.Provider value={{ username, setUsername }}>
        {props.children}
      </UserContext.Provider>
    );
  }

  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setLocalUsername(event.target.value);
  };

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    if (!isValidEmail) {
      return;
    }
    event.preventDefault();
    setUsername(localUsername); // Store username
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50 px-4 gap-2">
      <div className="max-w-md w-full space-y-8">
        <div>
          <h2 className="mt-6 text-center text-3xl font-extrabold text-gray-900">
            Welcome to the Hamilton UI!
          </h2>
          <p className="mt-4 text-center text-sm text-gray-600">
            Please enter an email address
          </p>
        </div>
        <form className="mt-8 space-y-6" onSubmit={handleSubmit}>
          <input
            type="text"
            autoComplete="username"
            name="username"
            value={username}
            onChange={handleChange}
            placeholder="Username"
            required
            className="appearance-none rounded-md relative block w-full px-3 py-2 border border-gray-300 placeholder-gray-500 text-gray-900 rounded-t-md focus:outline-none focus:ring-dwdarkblue focus:border-dwdarkblue focus:z-10 sm:text-sm"
          />
          <button
            type="submit"
            className={`group relative w-full flex justify-center py-2 px-4 border border-transparent text-sm font-medium rounded-md text-white ${isValidEmail ? "bg-dwdarkblue" : "bg-dwdarkblue/50"}  focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-dwdarkblue`}
            disabled={!isValidEmail}
          >
            Get started!
          </button>
        </form>
      </div>
    </div>
  );
};

export default LocalLoginProvider;
