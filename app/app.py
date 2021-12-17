import string
import pandas as pd


BASE_URL = "https://public.wiwdata.com/engineering-challenge/data"
OUTPUT_FILE_NAME = "out.csv"


def get_df(file_names: list[str]) -> pd.DataFrame:
    """Retrieve CSV in the form of Pandas DataFrames.

    Parameters: file_names - A list of file names to be retrieved from the URL.
    Returns: DataFrame with the transformed CSV data.
    """
    data = pd.concat([pd.read_csv(f"{BASE_URL}/{file}") for file in file_names])
    data = data.sort_values("user_id")
    return data


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Remove unnecessary columns from our data

    Parameters:
        df - DataFrame that we will remove unnecessary data from.
    Returns: Cleaned DataFrame
    """
    del df["drop"]
    del df["user_agent"]
    return df


def update_dict(dictionary: dict[str, int], row: tuple):
    """Abstraction function to document how the row series will update the dict

    Parameters:
        dictionary - The dictionary to be updated
        row - The row containing the new information
    """
    temp_path = row[1][1]
    temp_length = row[1][0]
    dictionary.update({temp_path: temp_length})


def populate_unseen_paths(dictionary: dict[str, int], paths: list[str]):
    """Populates paths unseen by the user so we don't have holes in our data.

    Parameters:
        dictionary - Dictionary holding the current path/length values
        paths - Paths expected and to be checked against
    """
    for path in paths:
        if path not in dictionary.keys():
            dictionary.update({path: 0})


def extract_uid(group: pd.DataFrame, dictionary: dict[str, int]):
    """Abstraction function to document extracting the user ID out of the DataFrame

    Parameters:
        group - The grouped data in which to find the user ID
        dictionary - Destination for the user ID
    """
    id = group["user_id"].reset_index(drop=True)[0]
    dictionary.update({"user_id": id})
    group.pop("user_id")


if __name__ == "__main__":
    # Create the list of csv names to pull
    file_names = [letter + ".csv" for letter in string.ascii_lowercase]
    data = get_df(file_names)
    data = clean(data)
    # Create our list of headers for the CSV
    headers = ["user_id"]
    paths = data["path"].unique()
    headers.extend(paths)
    out = None  # We will set to None first so that we can skip the first merge
    temp_dict = {}  # Temporary holding variable for our data
    grouped = data.groupby("user_id")
    for _, group in grouped:
        extract_uid(group, temp_dict)
        for row in group.iterrows():
            update_dict(temp_dict, row)
        populate_unseen_paths(temp_dict, paths)
        # Now comes the fun part - making sure the DataFrames merge
        df = pd.DataFrame([temp_dict], columns=headers)
        if out is not None:
            out = pd.concat([out, df])
        else:
            out = pd.DataFrame([temp_dict], columns=headers)
        temp_dict = {}  # Reset temporary dictionary for more data
    out.to_csv(OUTPUT_FILE_NAME, index=False)
