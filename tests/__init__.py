import os

from vcr import VCR

recording_path = os.path.join(os.path.dirname(__file__), 'recordings')

def _build_func_path_generator(function):
    import inspect
    module = os.path.basename(inspect.getfile(function)).replace('.py', '')
    return module + '/' + function.__name__

my_vcr = VCR(
    cassette_library_dir=recording_path,
    record_mode="once",
    func_path_generator=_build_func_path_generator,
    path_transformer=VCR.ensure_suffix('.yaml'),
    filter_headers=['authorization'],
    )
