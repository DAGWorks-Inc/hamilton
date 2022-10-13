# This script is used to create separate python virtual environments for individual examples
# A python virtual environment named "hamilton-env" is created in every directory containing requirements.txt file

# USAGE (inside hamilton/examples directory): bash make_python_virtualenv.sh

# Get a list of all the folders containing "requirements.txt" file
export folders=$(find . -name 'requirements.txt' -printf '%h\n');

echo "List of all folders containing requirements.txt";
echo $folders;

for folder in $folders; do
    # Change directory
    pushd $folder;

    # Remove previous hamilton python virtual environment
    rm -rf ./hamilton-env;

    # Create a new python virtual environment named "hamilton"
    python3 -m venv hamilton-env;

    # Change to that virtual environment
    source ./hamilton-env/bin/activate;

    # Install the requirements listed in hamilton virtual environment
    pip install -r requirements.txt;

    # Deactivate the virtual environment
    deactivate;

    # Return to the examples folder
    popd;
done
