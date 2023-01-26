"""Common project paths."""

from pathlib import Path


def get_project_root() -> Path:
    """Should be edited if moved from //housing-nyc/housing_nyc/tools/."""
    return Path(__file__).parent.parent.parent


def path_str(path_to_obj):
    """
    Convert pathlib Path to str without needing extra line to Path.joinpath.
    If path_to_obj is empty str, return path to root of project.

    Note that this leaves out any hanging forward/back slashes in path_to_obj
    """

    ROOT_PATH = get_project_root()
    if path_to_obj:
        path_obj = ROOT_PATH.joinpath(path_to_obj)
        return str(path_obj)
    else:
        return str(ROOT_PATH)


root_path = path_str("")
gtfs_feed_path = path_str("data/external/gtfs-feed-nyc")
config_path = path_str("config.ini")