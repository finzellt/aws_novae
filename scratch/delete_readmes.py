import os

def delete_blankreadme_files(directory):
    try:
        entries = os.listdir(directory)
    except Exception as e:
        print(f"Error accessing {directory}: {e}")
        return

    for entry in entries:
        path = os.path.join(directory, entry)
        if os.path.isdir(path):
            # Recursively process subdirectories
            delete_blankreadme_files(path)

    # After processing subdirectories, check for "blankreadme.txt" in the current directory
    blankreadme_path = os.path.join(directory, "blankreadme.txt")
    if os.path.isfile(blankreadme_path):
        try:
            os.remove(blankreadme_path)
            print(f"Deleted: {blankreadme_path}")
        except Exception as e:
            print(f"Error deleting {blankreadme_path}: {e}")

delete_blankreadme_files("/Users/tfinzell/Git/aws_novae/Individual_Novae")