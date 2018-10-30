To run the test suite against the published package:

    py.test -x -vvv --doctest-modules --pyargs azure.datalake.store tests

To run the test suite against a local build:
    
    python setup.py develop
    
    py.test -x -vvv --doctest-modules --pyargs azure.datalake.store tests
    
This test suite uses [VCR.py](https://github.com/kevin1024/vcrpy) to record the
responses from Azure. Borrowing from VCR's
[usage](https://vcrpy.readthedocs.io/en/latest/usage.html#record-modes), this
test suite has four recording modes: `once`, `new_episodes`, `none`, `all`. The
recording mode can be changed using the `RECORD_MODE` environment variable when
invoking the test suite (defaults to `none`).

To record responses for a new test without updating previous recordings:

    RECORD_MODE=once py.test -x -vvv --doctest-modules --pyargs azure-datalake-store tests

To record responses for all tests even if previous recordings exist:

    RECORD_MODE=all py.test -x -vvv --doctest-modules --pyargs azure-datalake-store tests

When recording new responses, you will need valid Azure credentials. The following
environment variables should be defined:

* `azure_username`
* `azure_password`
* `azure_data_lake_store_name`
* `azure_subscription_id`
* `azure_resource_group_name`
* `azure_service_principal`
* `azure_service_principal_secret`

Optionally, you may need to define `azure_tenant_id` or `azure_data_lake_store_url_suffix`.
