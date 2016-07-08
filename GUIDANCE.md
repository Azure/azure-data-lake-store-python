# Links / guidance for Data Lake Python SDKs

_Last updated: 7/8/2016_

## Authorization & authentication

### Authentication

Users will use the following package to authenticate as the first step when dealing with the SDK:

 - PyPI:  [https://pypi.python.org/pypi/adal/](https://pypi.python.org/pypi/adal/)
 - GitHub:  [https://github.com/AzureAD/azure-activedirectory-library-for-python](https://github.com/AzureAD/azure-activedirectory-library-for-python)

### Authorization (Azure RBAC)

Users will use the following package (currently just the stable version) to manage which users/groups can access which Azure resources, using Azure Resource Manager (ARM)’s Role-Based Access Control (RBAC):

 - PyPI:  [https://pypi.python.org/pypi/azure-mgmt-resource/](https://pypi.python.org/pypi/azure-mgmt-resource/)
 - GitHub:  [https://github.com/Azure/azure-sdk-for-python/tree/master/azure-mgmt-resource](https://github.com/Azure/azure-sdk-for-python/tree/master/azure-mgmt-resource)
 - Docs:  [https://azure-sdk-for-python.readthedocs.io/en/latest/resourcemanagement.html](https://azure-sdk-for-python.readthedocs.io/en/latest/resourcemanagement.html)

### Authorization (WebHDFS ACLs / permissions)

Users will use the Azure Data Lake Store SDK to manage which users/groups can access which files/directories.

## Development

### GitHub repository locations

The source code should be placed in the following GitHub repositories, with a note indicating that it is under development.
 
 * ADLS control plane and ADLA: [https://github.com/Azure/azure-sdk-for-python](https://github.com/Azure/azure-sdk-for-python)
    * For examples of the desired usage patterns, please see other services’ sections in this repository.
    * Please note that, for now, this code will be auto-generated from the Azure SDK tools, instead we will be focusing on the data plane.
 * ADLS data plane: [https://github.com/Azure/azure-data-lake-store-python](https://github.com/Azure/azure-data-lake-store-python)
    * For examples of the desired usage patterns, please see the [Azure Storage SDK](https://github.com/Azure/azure-storage-python).

### Code check-in process

Please follow the standard GitHub PR process:
 
 * Fork the main azure repository
 * Make changes in your fork
 * Submit those changes as a PR to the main Azure/* repo.

### Continuous integration and testing
      
It is critical that all PRs are tested as part of the PR process. To that end we are using Travis as our continuous integration harness. I leave it up to you how to author your test cases for the changes, provided that we have adequate coverage and those tests are run during CI for each PR and that new tests can be easily added. For a sample of how CI works for the rest of the python clients [please see this repo](https://github.com/lmazuel/swagger-to-sdk/blob/master/.travis.yml).
      
It is recommended from the Python team that we use nosetests as a test launcher and “coverage” to get a code coverage report, which is plugged with coveralls, but again, I leave the implementation to you so long as we have reliable tests and good code coverage.

## Sample authentication

Authentication is always the first step when initializing the use of the ADLS or ADLA SDKs.

Users will authenticate with AAD using the Active Directory Authentication Library (ADAL) for Python. There are many ways that the user can authenticate, but one (service principal authentication using secret key) is shown in the following example: [https://github.com/Azure-Samples/resource-manager-python-resources-and-groups](https://github.com/Azure-Samples/resource-manager-python-resources-and-groups)
