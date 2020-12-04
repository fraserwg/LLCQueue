import numpy as np
import yaml
from filelock.filelock import FileLock

DEFAULT_FILE_PATH = "/data/ocean/sann6294/llc4320/data/folder_status.yml"

def _open_database(filepath=DEFAULT_FILE_PATH):
    """ should always be called with a FileLock context manager
    """
    with open(filepath, 'r') as file:
        return yaml.safe_load(file)


def _save_database(database, filepath=DEFAULT_FILE_PATH):
    """ should always be called with a FileLock context manager
    """
    with open(filepath, 'w') as file:
        yaml.dump(database, file, indent=4)


def process_failure(itern, variable, process, filepath=DEFAULT_FILE_PATH):
    with FileLock(filepath):
        database = _open_database(filepath)
        _process_failure(database, itern, variable, process, filepath)


def process_success(itern, variable, process, filepath=DEFAULT_FILE_PATH):
    with FileLock(filepath):
        database = _open_database(filepath)
        _process_success(database, itern, variable, process, filepath)
            

def process_commence(itern, variable, process, filepath=DEFAULT_FILE_PATH):
    with FileLock(filepath):
        database = _open_database(filepath)
        _process_commence(database, itern, variable, process, filepath)


def _process_failure(database, itern, variable, process, filepath=DEFAULT_FILE_PATH):
    database[process][variable]['to_do'].append(itern)  # Add to to_do
    database[process][variable]['in_progress'].remove(itern)  # Remove from in_progress
    _save_database(database, filepath) # Save the database

    
def _process_success(database, itern, variable, process, filepath=DEFAULT_FILE_PATH):
    database[process][variable]['completed'].append(itern)  # Add to completed
    database[process][variable]['in_progress'].remove(itern)  # Remove from in_progress
    _save_database(database, filepath) # Save the database


def _process_commence(database, itern, variable, process, filepath=DEFAULT_FILE_PATH):
    database[process][variable]['in_progress'].append(itern)  # Add to in_progress
    database[process][variable]['to_do'].remove(itern)  # Remove from to_do
    _save_database(database, filepath)  # Save the database


def append_to_do(itern_list, variable, process, filepath=DEFAULT_FILE_PATH):
    with FileLock(filepath):
        database = _open_database(filepath)
        _append_to_do(database, itern_list, variable, process)


def _append_to_do(database, itern_list, variable, process, filepath=DEFAULT_FILE_PATH):
    # Sort the type of itern_list
    if isinstance(itern_list, np.ndarray):
        itern_list = itern_list.tolist()
    elif isinstance(intern_list, int):
        itern_list = [itern_list]
    else:
        msg = "itern_list should be of type list, not {}".format(type(itern_list))
        raise TypeError(msg)

    # Remove completed, in progress or already queued iterations
    completed_list = database[process][variable]['completed']
    in_progress_list = database[process][variable]['in_progress']
    current_to_do_list = database[process][variable]['to_do']

    itern_list = [itern for itern in intern_list if itern not in completed_list]
    itern_list = [itern for itern in intern_list if itern not in in_progress_list]
    itern_list = [itern for itern in intern_list if itern not in current_to_do_list]
    
    # Update the database
    database[process][variable]['to_do'] += itern_list
    _save_database(database, filepath)


def update_vorticity_to_do(filepath=DEFAULT_FILE_PATH):
    with FileLock(filepath):
        database = _open_database(filepath)
        
        # Check for completed or in progress velocity and vorticity files
        completed_vel = database['downloads']['velocity']['completed']
        completed_vort = database['post_processing']['vorticity']['completed']
        in_progress_vort = database['post_processing']['vorticity']['in_progress']

        # Determine files available for vorticity calculation, and update the database
        to_do_vort = [itern for itern in completed_vel if itern not in completed_vort]
        to_do_vort = [itern for itern in to_do_vort if itern not in in_progress_vort]
        database['post_processing']['vorticity']['to_do'] = to_do_vort
        _save_database(database, filepath)


def update_buoyancy_to_do(filepath=DEFAULT_FILE_PATH):
    with FileLock(filepath):
        database = _open_database(filepath)
        
        # Check for completed or in progress density download and processed files
        completed_download_density = database['downloads']['density']['completed']
        completed_processed_buoyancy = database['post_processing']['buoyancy']['completed']
        in_progress_processed_buoyancy = database['post_processing']['buoyancy']['in_progress']

        # Determine files available for vorticity calculation, and update the database
        to_do_processed_buoyancy = [itern for itern in completed_download_density if itern not in completed_processed_buoyancy]
        to_do_proceessed_buoyancy = [itern for itern in to_do_processed_buoyancy if itern not in in_progress_processed_buoyancy]
        database['post_processing']['buoyancy']['to_do'] = to_do_proceessed_buoyancy
        _save_database(database, filepath)

        
def update_pv_to_do(filepath=DEFAULT_FILE_PATH):
    with FileLock(filepath):
        database = _open_database(filepath)
        
        # Check for completed vorticity and density files, find the intersection
        completed_processed_buoyancy = database['post_processing']['buoyancy']['completed']
        completed_processed_vorticity = database['post_processing']['vorticity']['completed']
        to_do_pv_processing = [itern for itern in completed_processed_buoyancy if itern in completed_processed_vorticity]

        # Check for in progress and completed PV files, then remove from the list
        pv_in_progress = database['post_processing']['potential_vorticity']['in_progress']
        pv_completed = database['post_processing']['potential_vorticity']['completed']
        to_do_pv_processing = [itern for itern in to_do_pv_processing if itern not in pv_in_progress]
        to_do_pv_processing = [itern for itern in to_do_pv_processing if itern not in pv_completed]

        # Update the to_do list
        database['post_processing']['potential_vorticity']['to_do'] = to_do_pv_processing
        _save_database(database, filepath)
