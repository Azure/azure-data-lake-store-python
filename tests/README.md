To run the test suite:

    py.test -x -vvv --doctest-modules --pyargs adlfs tests

This test suite uses [VCR.py](https://github.com/kevin1024/vcrpy) to record the
responses from Azure. Borrowing from VCR's
[usage](https://vcrpy.readthedocs.io/en/latest/usage.html#record-modes), this
test suite has four recording modes: `once`, `new_episodes`, `none`, `all`. The
recording mode can be changed using the `RECORD_MODE` environment variable when
invoking the test suite (defaults to `none`).

To record responses for a new test without updating previous recordings:

    RECORD_MODE=once py.test -x -vvv --doctest-modules --pyargs adlfs tests

To record responses for all tests even if previous recordings exist:

    RECORD_MODE=all py.test -x -vvv --doctest-modules --pyargs adlfs tests
