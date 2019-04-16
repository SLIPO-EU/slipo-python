"""
SLIPO API entry point.

This class provides access to all SLIPO API functionality for accessing the user 
file system, querying the catalog, querying existing workflows and executing SLIPO 
Toolkit operations.
"""
import warnings

from typing import Union

from .exceptions import SlipoException

from .filesystem import FileSystemClient
from .catalog import CatalogClient
from .process import ProcessClient
from .operation import OperationClient, EnumDataFormat

# Default API endpoint
BASE_URL = 'https://app.dev.slipo.eu/'


class Client(object):
    """Class implementing all SLIPO API functionality

    Args:
        api_key (str): SLIPO API key. An application key can be generated using
            the SLIPO Workbench application.
        base_url (str, optional): Base URL for SLIPO API endpoints. The default
            value is ``https://app.dev.slipo.eu/``.
        requires_ssl (bool, optional): If `False`, unsecured connections are allowed (default `True`).

    Returns:
        A :py:class:`Client <slipo.client.Client>` object.

    """

    def __init__(self,  api_key, base_url=None, requires_ssl=True):
        self.base_url = self._check_base_url(base_url, requires_ssl)

        self.file_client = FileSystemClient(base_url, api_key)
        self.catalog_client = CatalogClient(base_url, api_key)
        self.process_client = ProcessClient(base_url, api_key)
        self.operation_client = OperationClient(base_url, api_key)

    def _check_base_url(self, base_url, requires_ssl):
        if not base_url.startswith("https"):
            if requires_ssl == False:
                warnings.warn('You are using an API key over an unsecured '
                              'connection!!!')
            else:
                raise SlipoException('HTTPS should be used for API requests')

            # Append a trailing / if not one exists
        if not base_url.endswith('/'):
            base_url += '/'

        return base_url

    def file_browse(self):
        """Browse all files and folders on the remote file system.

        Returns:
            A :obj:`dict` representing the parsed JSON response.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        return self.file_client.browse()

    def file_download(self, source: str, target: str):
        """Download a file from the remote file system.

        Args:
            source (str): Relative file path on the remote file system.
            target (str): The path where to save the file.

        Raises:
            SlipoException: If a network, server error or I/O error has occurred.
        """

        return self.file_client.download(source, target)

    def file_upload(self, source: str, target, overwrite=False):
        """Upload a file to the remote file system.

        Note:
            File size constraints are enforced on the uploaded file. The default
            installation allows files up to 20 Mb. 
            
            Moreover, space quotas are applied on the server. The default user
            space is 5GB.

            Directory nesting constraints are applied for the ``target`` value. The
            default installation allows nesting of directories up to 5 levels.

        Args:
            source (str): The path of the file to upload.
            target (str): Relative path on the remote file system where to save the
                file. If the directory does not exist, it will be created.
            overwrite (bool, optional): Set true if the operation should overwrite
                any existing file.

        Raises:
            SlipoException: If a network, server error or I/O error has occurred.
        """

        return self.file_client.upload(source, target, overwrite=overwrite)

    def catalog_query(self, term: str = None, pageIndex: int = 0, pageSize: int = 10):
        """Query resource catalog for RDF datasets.

        Args:
            term (str, optional): A term for filtering resources. If specified, 
                only the resources whose name contains the term are returned.
            pageIndex (str, optional): Page index for data pagination.
            pageSize (str, optional): Page size for data pagination.

        Returns:
            A :obj:`dict` representing the parsed JSON response.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        return self.catalog_client.query(term=term, pageIndex=pageIndex, pageSize=pageSize)

    def catalog_download(self, resource_id: int, resource_version: int, target: str):
        """Download resource to the local file system

        Args:
            resource_id (int): The resource id.
            resource_version (int): The resource revision.
            target (str): The path where to save the file.

        Raises:
            SlipoException: If a network, server error or I/O error has occurred.
        """

        return self.catalog_client.download(resource_id, resource_version, target)

    def process_query(self, term: str = None, pageIndex: int = 0, pageSize: int = 10):
        """Query workflow instances.

        Args:
            term (str, optional): A term for filtering workflows. If specified, 
                only the workflows whose name contains the term are returned.
            pageIndex (str, optional): Page index for data pagination.
            pageSize (str, optional): Page size for data pagination.

        Returns:
            A :obj:`dict` representing the parsed JSON response.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        return self.process_client.query(term=term, pageIndex=pageIndex, pageSize=pageSize)

    def process_start(self, process_id: int, process_version: int):
        """Start or resume execution of a workflow instance.

        Args:
            process_id (int): The process id.
            process_version (int): The process revision.

        Returns:
            A :obj:`dict` representing the parsed JSON response.

        Raises:
            SlipoException: If a network or server error has occurred.
        """
        return self.process_client.start(process_id, process_version)

    def process_stop(self, process_id: int, process_version: int):
        """Stop a running workflow execution instance.

        Args:
            process_id (int): The process id.
            process_version (int): The process revision.

        Returns:
            A :obj:`dict` representing the parsed JSON response.

        Raises:
            SlipoException: If a network or server error has occurred.
        """
        return self.process_client.stop(process_id, process_version)

    def process_status(self, process_id: int, process_version: int):
        """Check the status of a workflow execution instance.

        Args:
            process_id (int): The process id.
            process_version (int): The process revision.

        Returns:
            A :obj:`dict` representing the parsed JSON response.

        Raises:
            SlipoException: If a network or server error has occurred.
        """
        return self.process_client.status(process_id, process_version)

    def process_file_download(self, process_id: int, process_version: int, file_id: int, target: str):
        """Download an input or output file for a specific workflow execution instance.

        During the execution of a workflow the following file types may be created:
            - ``CONFIGURATION``: Tool configuration
            - ``INPUT``: Input file
            - ``OUTPUT``: Output file
            - ``SAMPLE``: Sample data collected during step execution
            - ``KPI``: Tool specific or aggregated KPI data
            - ``QA``: Tool specific QA data
            - ``LOG``: Logs recorded during step execution 

        Args:
            process_id (int): The process id.
            process_version (int): The process revision.
            file_id (int): The file id.
            target (str): The path where to save the file.

        Raises:
            SlipoException: If a network, server error or I/O error has occurred.
        """

        return self.process_client.download(process_id, process_version, file_id, target)

    def profiles(self):
        """Browse all SLIPO Toolkit components profiles.

        Returns:
            A :obj:`dict` representing the parsed JSON response.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        return self.operation_client.profiles()

    def transform_csv(
        self,
        path: str,
        profile: str,
        **kwargs
    ):
        """Transforms a CSV file to a RDF dataset.

        Args:
            path (str): The relative path for a file on the remote user file
                system.
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            **kwargs: Keyword arguments to control the transform operation. Options are:

                - **featureSource** (str, optional): Specifies the data source provider of the
                  input features.
                - **encoding** (str, optional): The encoding (character set) for strings in the
                  input data (default: `UTF-8`)
                - **attrKey** (str, optional): Field name containing unique identifier for each
                  entity (e.g., each record in the shapefile).
                - **attrName** (str, optional): Field name containing name literals
                  (i.e., strings).
                - **attrCategory** (str, optional): Field name containing literals regarding
                  classification into categories (e.g., type of points, road classes etc.)
                  for each feature.
                - **attrGeometry** (str, optional): Parameter that specifies the name of the
                  geometry column in the input dataset.
                - delimiter (str, optional): Specify the character delimiting attribute
                  values.
                - **quote** (str, optional): Specify quote character for string values.
                - **attrX** (str, optional): Specify attribute holding X-coordinates of
                  point locations.
                - **attrY** (str, optional): Specify attribute holding Y-coordinates of
                  point locations.
                - **sourceCRS** (str, optional): Specify the EPSG numeric code for the
                  source CRS (default: `EPSG:4326`).
                - **targetCRS** (str, optional): Specify the EPSG numeric code for the
                  target CRS (default: `EPSG:4326`).
                - **defaultLang** (str, optional): Default lang for the labels created
                  in the output RDF (default: `en`).

        Returns:
            A :obj:`dict` representing the parsed JSON response.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        return self.operation_client.transform_csv(
            path,
            profile,
            **kwargs
        )

    def transform_shapefile(
        self,
        path: str,
        profile: str,
        **kwargs
    ):
        """Transforms a SHAPEFILE file to a RDF dataset.

        Args:
            path (str): The relative path for a file on the remote user file
                system.
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            **kwargs: Keyword arguments to control the transform operation. Options
                are:

                - **featureSource** (str, optional): Specifies the data source provider of the
                  input features.
                - **encoding** (str, optional): The encoding (character set) for strings in the
                  input data (default: `UTF-8`)
                - **attrKey** (str, optional): Field name containing unique identifier for each
                  entity (e.g., each record in the shapefile).
                - **attrName** (str, optional): Field name containing name literals
                  (i.e., strings).
                - **attrCategory** (str, optional): Field name containing literals regarding
                  classification into categories (e.g., type of points, road classes etc.)
                  for each feature.
                - **attrGeometry** (str, optional): Parameter that specifies the name of the
                  geometry column in the input dataset.
                - **sourceCRS** (str, optional): Specify the EPSG numeric code for the
                  source CRS (default: `EPSG:4326`).
                - **targetCRS** (str, optional): Specify the EPSG numeric code for the
                  target CRS (default: `EPSG:4326`).
                - **defaultLang** (str, optional): Default lang for the labels created
                  in the output RDF (default: `en`)

        Returns:
            A :obj:`dict` representing the parsed JSON response.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        return self.operation_client.transform_shapefile(
            path,
            profile,
            **kwargs
        )

    def interlink(
        self,
        profile: str,
        left: Union[str, tuple],
        right: Union[str, tuple]
    ):
        """Generates links for two RDF datasets.

        Arguments `left`, `right` and `links` may be either a :obj:`dict` or
        a :obj:`tuple` of two integer values. The former represents the relative
        path to the remote user file system, while the latter the id and revision
        of a catalog resource.

        Args:
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            left (Union[str, Tuple[int, int]]): The `left` RDF dataset.
            right (Union[str, Tuple[int, int]]): The `right` RDF dataset.

        Returns:
            A :obj:`dict` representing the parsed JSON response.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        return self.operation_client.interlink(profile, left, right)

    def fuse(
        self,
        profile: str,
        left: Union[str, tuple],
        right: Union[str, tuple],
        links: Union[str, tuple]
    ):
        """Fuses two RDF datasets using Linked Data and returns a new RDF dataset.

        Args:
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            left (Union[str, Tuple[int, int]]): The `left` RDF dataset.
            right (Union[str, Tuple[int, int]]): The `right` RDF dataset.
            links (Union[str, Tuple[int, int]]): The links for the `left` and `right` datasets.

        Returns:
            A :obj:`dict` representing the parsed JSON response.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        return self.operation_client.fuse(profile, left, right, links)

    def enrich(
        self,
        profile: str,
        input: Union[str, tuple]
    ):
        """Enriches a RDF dataset.

        Args:
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            source (Union[str, Tuple[int, int]]): The RDF dataset to enrich.

        Returns:
            A :obj:`dict` representing the parsed JSON response.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        return self.operation_client.enrich(profile, input)
