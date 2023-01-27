""" Class containing all methods to extract data from log files.

Including methods to use generators to make file extraction more
efficient and streamlined.

NB. May be over-engineering, so progress on this may not be updated.

"""

class Transform:

    def __init__(self, files_generator=None, tweets_generator=None):
        self.files_generator = files_generator
        self.tweets_generator = tweets_generator
        return

    def _generate_files(self, location: str) -> os.DirEntry:
        """ returns a python generator of files.
        Uses os.scandir() method to return a generator object

        Args:
            location: string, the fqdn of the folder/location

        Returns:
            os.DirEntry - generator object of files as a list of strings
                properties .path and .name are accessible
        """
        return os.scandir(location)

