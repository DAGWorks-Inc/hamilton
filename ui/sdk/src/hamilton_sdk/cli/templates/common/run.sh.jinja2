# basic script to load up any necessary environment variables

# Define the list of Python aliases to check
python_aliases=("python" "python3")

# Loop through the aliases and check if they exist
for alias in "${python_aliases[@]}"; do
    if command -v "$alias" >/dev/null 2>&1; then
        selected_alias="$alias"
        break
    fi
done

# Check if a valid alias was found
if [[ -n "$selected_alias" ]]; then
    echo "Executing $alias run.py"
    export $(grep -v '^#' .env | xargs) && $selected_alias run.py
    exit 0
else
    # If no valid alias is found, display an error message
    echo "No valid Python installation found. Please install Python or modify this script."
    exit 1
fi
