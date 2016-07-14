- First milestone: Core data
     lake store filesystem client, which includes:
    - 1:1 mapping of filesystem
      REST endpoints to client methods
    - Unit tests and CI tests
      for the core client covering all the methods
    - Pythonic Filesystem layer, including file class interfacing for the REST calls. This allows native interaction with file objects the same way a user would interact with basic python files and folders.

- Second milestone: performant
     extension to the core client for single file upload and download
    - Basically, multi-part
      upload/download support for single files, with flexibility for the user
      to determine how performant/parallel the upload/download is, with smart
      defaults that will take advantage of their available network capacity.
    - Unit tests and CI tests
      for this functionality

- Third milestone: addition
     of folder and recursive support for performant upload/download and
     performance tests
    - Extending the single file
      upload/download to allow for folder and recursive folder
      upload/download 
    - Unit tests and CI tests
      for this functionality
    - Addition of performance
      tests to measure performance for large files, folders full of small and
      mixed file sizes. Once stable, we will integrate these tests into our
      existing performance testing and reporting service.

- Fourth milestone:
     Stabilization, Integration and documentation
    - Stabilize the work of the
      previous three milestones and ensure all tests and CI jobs have robust
      coverage
    - Integrate this custom
      functionality with the existing Azure SDK for python. This includes
      proper packaging and naming, ensure inclusion of any common dependencies
      for things like error handling (ideally this is done in an ongoing basis
      during development in milestone one, but just in case anything is missed
      it is fixed here).
    - Get the functionality
      ready for package publishing, which includes ensuring our getting started
      documentation, samples and readthedocs code documentation is ready and
      has been reviewed.

- Fifth milestone (if time
     remains): Convenience layer for auto-generated clients
    - This is much lower
      priority than the previous four milestones, but if we have time it would
      be good to go over the auto-generated client functionality for our other
      four clients and see if there are any good quality of life improvements
      we can make for users.
